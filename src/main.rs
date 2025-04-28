use async_compression::tokio::bufread::GzipDecoder;
use bytes::Bytes;
use chardetng::EncodingDetector;
use clap::{Parser, ValueHint};
use counter::Counter;
use futures_util::stream::{self, Stream, StreamExt, TryStream, TryStreamExt};
use log::{Level, LevelFilter, Log, Metadata, Record, info, set_logger, set_max_level};
use regex::Regex;
use reqwest::{Client, Response};
use reqwest_middleware::{ClientBuilder, ClientWithMiddleware};
use reqwest_retry::{RetryTransientMiddleware, policies::ExponentialBackoff};
use select::{
    document::Document,
    predicate::{Attr, Name, Predicate},
};
use serde::{de::DeserializeOwned, ser::Serialize};
use std::{
    borrow::ToOwned,
    io::{self, Write, stdout},
    marker::Send,
    num::TryFromIntError,
    ops::Add,
    path::{Path, PathBuf},
};
use tokio::{
    fs::try_exists,
    io::{AsyncBufRead, AsyncBufReadExt, AsyncReadExt, BufReader},
    task::{JoinError, spawn},
};
use tokio_stream::wrappers::LinesStream;
use tokio_util::io::StreamReader;
use unicode_segmentation::UnicodeSegmentation;
use url::Url;
use vow::VowAsync;
use warc::{RecordType, WarcHeader, WarcReader};

#[derive(Debug, thiserror::Error)]
enum Error {
    #[error("HTTP status error: {0}")]
    HttpStatus(reqwest::StatusCode),
    #[error("IO error: {0}")]
    IO(#[from] std::io::Error),
    #[error("tokio join error: {0}")]
    Join(#[from] JoinError),
    #[error("Error: No filename in path: {0}")]
    NoFilenameInPath(String),
    #[error("Error: No \"href\" in \"a\" tag: {0}")]
    NoHref(String),
    #[error("Error: No links found in {0}")]
    NoLinksFound(String),
    #[error("Error: No TLD in domain: {0}")]
    NoTLDInDomain(String),
    #[error("reqwest error: {0}")]
    Reqwest(#[from] reqwest::Error),
    #[error("reqwest_middleware error: {0}")]
    ReqwestMiddleware(#[from] reqwest_middleware::Error),
    #[error("serde_json error: {0}")]
    TryFromInt(#[from] TryFromIntError),
    #[error("Error: TLD does not exist: {0}")]
    TLDDoesNotExist(String),
    #[error("Error: WARC-Target-URI not in header: {0}")]
    TargetURINotInWARCHeader(String),
    #[error("Error parsing URL: {0}")]
    URLParse(#[from] url::ParseError),
    #[error("vow error: {0}")]
    Vow(#[from] vow::Error),
    #[error("warc error: {0}")]
    Warc(#[from] warc::Error),
    #[error("Filename doesn't end with \".warc.wet.gz\": {0}")]
    WrongFilenameExtension(String),
}

const MY_LOGGER: MyLogger = MyLogger;

struct MyLogger;

impl Log for MyLogger {
    fn enabled(&self, metadata: &Metadata) -> bool {
        metadata.level() <= Level::Info
    }

    fn log(&self, record: &Record) {
        if self.enabled(record.metadata()) {
            println!("{}", record.args());
        }
    }
    fn flush(&self) {
        stdout().flush().unwrap();
    }
}

// https://github.com/seanmonstar/reqwest/issues/2381
#[inline]
async fn create_gzip_decoder<R>(buf_reader: R) -> GzipDecoder<R>
where
    R: AsyncBufRead,
{
    let mut gzip_decoder = GzipDecoder::new(buf_reader);
    gzip_decoder.multiple_members(true);
    gzip_decoder
}

trait TrySum: TryStream {
    async fn try_sum(self) -> Result<Self::Ok, Self::Error>
    where
        Self::Ok: Add<Output = Self::Ok>;
}

impl<S> TrySum for S
where
    S: TryStreamExt,
    S::Ok: Add<Output = S::Ok> + Default,
{
    async fn try_sum(self) -> Result<Self::Ok, Self::Error> {
        self.try_fold(S::Ok::default(), |acc, x| async { Ok(acc + x) })
            .await
    }
}

async fn get(client: &ClientWithMiddleware, url: Url) -> Result<Response, Error> {
    let response = client.get(url).send().await?;
    if response.status().is_success() {
        Ok(response)
    } else {
        Err(Error::HttpStatus(response.status()))
    }
}

async fn get_url_stream_reader(
    client: &ClientWithMiddleware,
    url: Url,
) -> Result<StreamReader<impl Stream<Item = Result<Bytes, io::Error>>, Bytes>, Error> {
    Ok(StreamReader::new(
        get(client, url)
            .await?
            .bytes_stream()
            .map_err(io::Error::other),
    ))
}

async fn get_wet_paths_urls(
    client: &ClientWithMiddleware,
    index: Url,
) -> Result<impl Stream<Item = Url>, Error> {
    let crawl_id = Regex::new(r"^[[:space:]]*CC-MAIN-([3-9][0-9]{3}|2[1-9][0-9]{2}|20[2-9][0-9]|201[3-9])-[0-9]{2}[[:space:]]*$").unwrap();
    Document::from(get(client, index.clone()).await?.text().await?.as_str())
        .find(Name("a").and(Attr("href", ())))
        .filter_map(|a| {
            if crawl_id.is_match(a.text().as_str()) {
                Some(
                    a.attr("href")
                        .ok_or_else(|| Error::NoHref(a.html()))
                        .and_then(|href| Ok(index.join(href)?.join("wet.paths.gz")?)),
                )
            } else {
                None
            }
        })
        .collect::<Result<Vec<Url>, Error>>()
        .and_then(|urls| match urls.len() {
            0 => Err(Error::NoLinksFound(index.to_string())),
            1.. => Ok(tokio_stream::iter(urls)),
        })
}

async fn get_segment_urls(
    client: &ClientWithMiddleware,
    base_url: &Url,
    paths_url: Url,
) -> impl Stream<Item = Result<Url, Error>> {
    match get_url_stream_reader(client, paths_url).await {
        Ok(stream_reader) => {
            LinesStream::new(BufReader::new(create_gzip_decoder(stream_reader).await).lines())
                .map(|path| Ok(base_url.join(&path?)?))
                .left_stream()
        }
        Err(err) => tokio_stream::once(Err(err)).right_stream(),
    }
}

async fn segment_count(
    client: ClientWithMiddleware,
    segment_url: Url,
) -> Result<Counter<String>, Error> {
    info!("counting: {}", segment_url);
    let mut buffer: Vec<u8> = Vec::new();
    let mut gzip_decoder =
        create_gzip_decoder(get_url_stream_reader(&client, segment_url).await?).await;
    // TODO: async WARC reader
    gzip_decoder.read_to_end(&mut buffer).await?;
    let warc_reader = WarcReader::new(&buffer[..]);
    stream::iter(
        warc_reader
            .iter_records()
            .map(|maybe_record| {
                let record = maybe_record?;
                Ok(if record.warc_type() == &RecordType::Conversion {
                    let mut encoding_detector = EncodingDetector::new();
                    encoding_detector.feed(record.body(), true);
                    let target_uri =
                        Url::parse(&record.header(WarcHeader::TargetURI).ok_or_else(|| {
                            let (raw_record_header, _) = record.clone().into_raw_parts();
                            Error::TargetURINotInWARCHeader(raw_record_header.to_string())
                        })?)?;
                    let (decoded, _, _) = encoding_detector
                        .guess(
                            target_uri
                                .domain()
                                .map(|domain| {
                                    domain
                                        .rsplit('.')
                                        .next()
                                        .ok_or_else(|| Error::NoTLDInDomain(String::from(domain)))
                                        .and_then(|top_level_domain| {
                                            if tld::exist(top_level_domain) {
                                                Ok(top_level_domain.as_bytes())
                                            } else {
                                                Err(Error::TLDDoesNotExist(String::from(
                                                    top_level_domain,
                                                )))
                                            }
                                        })
                                })
                                .transpose()?,
                            true,
                        )
                        .decode(record.body());
                    UnicodeSegmentation::graphemes(decoded.as_ref(), true)
                        .map(String::from)
                        .collect()
                } else {
                    Counter::new()
                })
            }),
    )
    .try_sum()
    .await
}

fn url_to_json_filename(url: &Url) -> Result<PathBuf, Error> {
    let path = url.path(); // could try to reverse the percent-encoding but it shouldn't matter
    let filename = path
        .rsplit('/')
        .next()
        .ok_or_else(|| Error::NoFilenameInPath(path.to_string()))?;
    let name = filename
        .strip_suffix(".warc.wet.gz")
        .ok_or_else(|| Error::WrongFilenameExtension(filename.to_string()))?;
    Ok(PathBuf::from(name).with_extension("json"))
}

async fn save<T>(path: &Path, data: T) -> Result<T, Error>
where
    T: DeserializeOwned + Serialize + Send + ToOwned<Owned = T>,
{
    info!("saving: {}", path.to_string_lossy());
    Ok(VowAsync::open_tokio(path)
        .default(data)
        .overwrite_local()
        .json(false)
        .build()
        .await?
        .get()
        .to_owned())
}

async fn load<T>(path: &Path) -> Result<T, Error>
where
    T: DeserializeOwned + Serialize + Send + ToOwned<Owned = T>,
{
    info!("loading: {}", path.to_string_lossy());
    Ok(VowAsync::open_tokio(path)
        .deny_invalid()
        .keep_local()
        .with_type::<T>()
        .json(false)
        .build()
        .await?
        .get()
        .to_owned())
}

async fn process_segment(
    client: ClientWithMiddleware,
    url: Url,
    counts_directory: PathBuf,
) -> Result<Counter<String>, Error> {
    let json_path = counts_directory.join(url_to_json_filename(&url)?);
    if try_exists(&json_path).await? {
        load(&json_path).await
    } else {
        save(&json_path, segment_count(client, url).await?).await
    }
}

#[derive(Parser, Debug)]
#[command(version, about = "count characters in the common crawl", long_about = None)]
struct Args {
    // TODO:
    //    #[arg(long, default_value = "", value_hint = ValueHint::DirPath, help = "output file")]
    //    output: String,
    #[arg(
        long,
        default_value = "counts",
        value_hint = ValueHint::DirPath,
        help = "Directory where segment count files will be stored")]
    counts_directory: String,
    #[arg(
        short,
        long,
        default_value_t = num_cpus::get(),
        help = "The maximum number of concurrent tasks to run (default: number of CPU cores)",
        conflicts_with = "no_limit")]
    limit: usize,
    #[arg(long, help = "Disable concurrency limit")]
    no_limit: bool,
    #[arg(
        short,
        long,
        default_value_t = u32::MAX,
        help = "The maximum number of retries for network requests (default: 2^32-1)")]
    retries: u32,
    //    #[arg(short, long, default_value_t = false, help = "Show progress")]
    //    progress: bool,
    #[arg(
        short,
        long,
        default_value_t = false,
        help = "Print segments as they are processed"
    )]
    verbose: bool,
    // TODO:
    //#[arg(long, default_value_t = false, help = "Verify local data")]
    //verify: bool,
}

// TODO: retry io error
// TODO: make grand total optional to save memory
// TODO: Termination
#[tokio::main]
async fn main() -> Result<(), Error> {
    let args = Args::parse();
    set_logger(&MY_LOGGER).unwrap();
    set_max_level(if args.verbose {
        LevelFilter::Info
    } else {
        LevelFilter::Warn
    });
    let limit = if args.no_limit || args.limit == 0 {
        None
    } else {
        Some(args.limit)
    };
    let client = ClientBuilder::new(Client::new())
        .with(RetryTransientMiddleware::new_with_policy(
            ExponentialBackoff::builder().build_with_max_retries(args.retries),
        ))
        .build();
    let base_url = Url::parse("https://data.commoncrawl.org/").unwrap();
    let index = Url::parse("https://data.commoncrawl.org/crawl-data/index.html").unwrap();
    let counts_directory = PathBuf::from(args.counts_directory);
    save(
        &counts_directory.join("grand_total.json"),
        get_wet_paths_urls(&client, index)
            .await?
            .then(|url| get_segment_urls(&client, &base_url, url))
            .flatten_unordered(limit)
            .map(|url| async {
                let result: Result<Counter<String>, Error> = spawn(process_segment(
                    client.clone(),
                    url?,
                    counts_directory.clone(),
                ))
                .await?;
                result
            })
            .buffer_unordered(limit.unwrap_or(usize::MAX))
            .try_sum()
            .await?,
    )
    .await?;
    Ok(())
}
