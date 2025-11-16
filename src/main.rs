use async_compression::tokio::bufread::GzipDecoder;
use bytes::{Bytes, BytesMut};
use chardetng::EncodingDetector;
use clap::{Parser, ValueHint};
use core::str;
use counter::Counter;
use futures_util::{
    pin_mut,
    stream::{self, Stream, StreamExt, TryStream, TryStreamExt, try_unfold},
};
use log::{Level, LevelFilter, Log, Metadata, Record, info, set_logger, set_max_level, warn};
use regex::Regex;
use reqwest::header::{ACCEPT_RANGES, RANGE};
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
    marker::{Send, Unpin},
    num::TryFromIntError,
    ops::Add,
    path::{Path, PathBuf},
};
use tokio::{
    fs::{create_dir_all, try_exists}, // TODO: use is_file
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
    #[error("IO error: {0}")]
    IO(#[from] std::io::Error),
    #[error("tokio join error: {0}")]
    Join(#[from] JoinError),
    #[error("Error: No \"href\" in \"a\" tag: {0}")]
    NoHref(String),
    #[error("Error: No links found in {0}")]
    NoLinksFound(String),
    #[error("reqwest error: {0}")]
    Reqwest(#[from] reqwest::Error),
    #[error("reqwest_middleware error: {0}")]
    ReqwestMiddleware(#[from] reqwest_middleware::Error),
    #[error("Error: Tried skipping beyond end of stream")]
    StreamTooShort(),
    #[error("serde_json error: {0}")]
    TryFromInt(#[from] TryFromIntError),
    #[error("Error: WARC-Target-URI not in header: {0}")]
    TargetURINotInWARCHeader(String),
    #[error("Unknown filename scheme: {0}")]
    UnknownFilenameFormat(String),
    #[error("Error: Malformed URL path: {0}")]
    URLPath(String),
    #[error("Error parsing URL: {0}")]
    URLParse(#[from] url::ParseError),
    #[error("UTF8 Error: {0}")]
    UTF8(#[from] std::str::Utf8Error),
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

fn skip_bytes<S>(byte_stream: S, skip: usize) -> impl Stream<Item = Result<Bytes, Error>>
where
    S: Stream<Item = Result<Bytes, Error>> + Unpin,
{
    try_unfold((byte_stream, skip), |(mut stream, mut skip)| async move {
        match skip {
            0 => match stream.next().await {
                Some(item) => match item {
                    Ok(bytes) => Ok(Some((bytes, (stream, 0)))),
                    Err(error) => Err(error),
                },
                None => Ok(None),
            },
            1.. => {
                while let Some(chunk_result) = stream.next().await {
                    let chunk = chunk_result?;
                    let chunk_len = chunk.len();
                    if skip >= chunk_len {
                        skip -= chunk_len;
                        continue;
                    } else {
                        return Ok(Some((chunk.slice(skip..), (stream, 0))));
                    }
                }
                Err(Error::StreamTooShort())
            }
        }
    })
}

fn get_byte_stream(
    client: ClientWithMiddleware,
    url: Url,
) -> impl Stream<Item = Result<Bytes, Error>> {
    try_unfold(
        (client, url, None, 0usize),
        |(client, url, stream_opt, mut position)| async move {
            let (mut stream, use_range_request) = match stream_opt {
                Some((stream, use_range_request)) => (stream, use_range_request),
                None => {
                    let response = client.get(url.clone()).send().await.map_err(Error::from)?;
                    let use_range_request = response
                        .headers()
                        .get(ACCEPT_RANGES)
                        .map_or_else(|| false, |accept_ranges| accept_ranges == "bytes");

                    (
                        response.bytes_stream().map_err(Error::from).boxed(),
                        use_range_request,
                    )
                }
            };
            loop {
                match stream.next().await {
                    Some(Ok(bytes)) => {
                        position += bytes.len();
                        return Ok(Some((
                            bytes,
                            (client, url, Some((stream, use_range_request)), position),
                        )));
                    }
                    Some(Err(error)) => {
                        warn!("{error}");
                        warn!("Retrying...");
                        stream = if use_range_request {
                            client
                                .get(url.clone())
                                .header(RANGE, format!("bytes={position}-"))
                                .send()
                                .await
                                .map_err(Error::from)?
                                .bytes_stream()
                                .map_err(Error::from)
                                .boxed()
                        } else {
                            skip_bytes(
                                client
                                    .get(url.clone())
                                    .send()
                                    .await
                                    .map_err(Error::from)?
                                    .bytes_stream()
                                    .map_err(Error::from),
                                position,
                            )
                            .boxed()
                        };
                    }
                    None => return Ok(None),
                }
            }
        },
    )
    .boxed()
}

async fn get_bytes(client: &ClientWithMiddleware, url: Url) -> Result<Bytes, Error> {
    get_byte_stream(client.clone(), url)
        .try_collect::<BytesMut>()
        .await
        .map(|bytes| bytes.freeze())
}

async fn get_wet_paths_urls(
    client: &ClientWithMiddleware,
    index: Url,
) -> Result<impl Stream<Item = Url>, Error> {
    let crawl_id = Regex::new(r"^[[:space:]]*CC-MAIN-([3-9][0-9]{3}|2[1-9][0-9]{2}|20[2-9][0-9]|201[3-9])-[0-9]{2}[[:space:]]*$").unwrap();
    Document::from(str::from_utf8(&get_bytes(client, index.clone()).await?)?)
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
    LinesStream::new(
        BufReader::new(
            create_gzip_decoder(StreamReader::new(
                get_byte_stream(client.clone(), paths_url).map_err(io::Error::other),
            ))
            .await,
        )
        .lines(),
    )
    .map(|path| Ok(base_url.join(&path?)?))
}

async fn segment_count(
    client: ClientWithMiddleware,
    segment_url: Url,
) -> Result<Counter<String>, Error> {
    info!("counting: {segment_url}");
    let gzip_decoder = create_gzip_decoder(StreamReader::new(
        get_byte_stream(client, segment_url).map_err(io::Error::other),
    ))
    .await;
    pin_mut!(gzip_decoder);
    let mut buffer = Vec::new();
    gzip_decoder.read_to_end(&mut buffer).await?;
    // TODO: async WARC reader
    let warc_reader = WarcReader::new(&buffer[..]);
    let counts = warc_reader.iter_records().map(|maybe_record| {
        let record = maybe_record?;
        if record.warc_type() != &RecordType::Conversion {
            return Ok(Counter::new());
        }
        let mut encoding_detector = EncodingDetector::new();
        encoding_detector.feed(record.body(), true);
        let Some(target_uri) = record.header(WarcHeader::TargetURI) else {
            let (raw_record_header, _) = record.clone().into_raw_parts();
            return Err(Error::TargetURINotInWARCHeader(
                raw_record_header.to_string(),
            ));
        };
        let parsed_url = match Url::parse(&target_uri) {
            Ok(parsed_url) => Some(parsed_url),
            Err(url::ParseError::IdnaError) => {
                warn!("URL parse error: IDNA error: \"{}\"", &target_uri);
                None
            }
            Err(url::ParseError::InvalidIpv4Address) => {
                warn!("URL parse error: Invalid IPv4 address: \"{}\"", &target_uri);
                None
            }
            Err(parse_error) => return Err(Error::from(parse_error)),
        };
        let tld_bytes = parsed_url.as_ref().and_then(|url| {
            let domain = url.domain()?;
            let top_level = domain
                .strip_suffix('.')
                .unwrap_or(domain)
                .rsplit('.')
                .next()?;
            // TODO: what about discontinued TLDs?
            if tld::exist(top_level) {
                Some(top_level.as_bytes())
            } else {
                warn!("Invalid TLD: \"{top_level}\" (URI: \"{target_uri}\")");
                None
            }
        });
        let (decoded, _, _) = encoding_detector
            .guess(tld_bytes, true)
            .decode(record.body());
        Ok(UnicodeSegmentation::graphemes(decoded.as_ref(), true)
            .map(String::from)
            .collect())
    });
    stream::iter(counts).try_sum().await
}

fn url_to_path(url: &Url) -> Result<(PathBuf, PathBuf), Error> {
    let path = url.path(); // could try to reverse the percent-encoding but it shouldn't matter
    let mut iter = path.split('/');
    if let (Some(""), Some("crawl-data"), Some(crawl_name), Some(url_filename)) =
        (iter.next(), iter.next(), iter.next(), iter.next_back())
    {
        let json_filename = PathBuf::from(
            [
                url_filename
                    .strip_suffix(".warc.wet.gz")
                    .ok_or_else(|| Error::WrongFilenameExtension(url_filename.to_string()))?,
                "json",
            ]
            .join("."), // TODO: use .with_added_extension() when it comes out of nightly
        );
        // could try to validate the date but that way madness lies
        if !Regex::new(r"^(?:CC-MAIN-[0-9]{14}-[0-9]{14}-[0-9]{5}|CC-MAIN-[0-9]{14}-[0-9]{5}-ip(?:-(?:25[0-5]|2[0-4][0-9]|1[0-9]{2}|[1-9]?[0-9])){4}.ec2.internal).warc.wet.gz$").unwrap().is_match(url_filename) {
            return Err(Error::UnknownFilenameFormat(url_filename.to_string()));
        }
        Ok((
            PathBuf::from(crawl_name).join(&url_filename[..22]),
            json_filename,
        ))
    } else {
        Err(Error::URLPath(path.to_string()))
    }
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
    let (crawl_name, json_filename) = url_to_path(&url)?;
    let subdirectory = counts_directory.join(crawl_name);
    create_dir_all(&subdirectory).await?;
    let json_path = subdirectory.join(&json_filename);
    if try_exists(&json_path).await? {
        load(&json_path).await // TODO: don't load if we're not summing
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
    #[arg(
        short,
        long,
        default_value_t = true,
        help = "Sum all segments (default)",
        conflicts_with = "no_total"
    )]
    total: bool,
    #[arg(long, help = "Don't sum segments")]
    no_total: bool,
    #[arg(long, help = "Only count the number of segments")]
    count_segments: bool,
    // TODO:
    //#[arg(long, default_value_t = false, help = "Verify local data")]
    //verify: bool,
}

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
    let total = if args.no_total { false } else { args.total };
    let retry_policy = ExponentialBackoff::builder().build_with_max_retries(args.retries);
    let client = ClientBuilder::new(reqwest::Client::new())
        .with(RetryTransientMiddleware::new_with_policy(retry_policy))
        .build();
    let base_url = Url::parse("https://data.commoncrawl.org/").unwrap();
    let index = Url::parse("https://data.commoncrawl.org/crawl-data/index.html").unwrap();
    let counts_directory = PathBuf::from(args.counts_directory);
    // TODO: store in trie?
    let segment_urls: Vec<Url> = get_wet_paths_urls(&client, index)
        .await?
        .then(|url| get_segment_urls(&client, &base_url, url))
        .flatten_unordered(limit)
        .try_collect()
        .await?;
    // TODO: add progress bar
    info!("found {} segment URLs", segment_urls.len());
    if args.count_segments {
        println!("{}", segment_urls.len());
        return Ok(());
    }
    let counts = stream::iter(segment_urls.into_iter().rev())
        .map(|url| async {
            spawn(process_segment(
                client.clone(),
                url,
                counts_directory.clone(),
            ))
            .await? as Result<Counter<String>, Error>
        })
        .buffer_unordered(limit.unwrap_or(usize::MAX));
    if total {
        save(
            &counts_directory.join("grand_total.json"),
            counts.try_sum().await?,
        )
        .await?;
    } else {
        counts.try_for_each(|_| async { Ok(()) }).await?;
    };
    Ok(())
}
