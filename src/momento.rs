use rustls::pki_types::ServerName;
use tokio_rustls::TlsConnector;
use rustls::RootCertStore;
use http::Uri;
use bytes::Bytes;
use bytes::BytesMut;
use clap::Parser;
use clap::ValueEnum;
use http::{Method, Request};
use rand::RngCore;
use rand::SeedableRng;
use rand_xoshiro::Seed512;
use rand_xoshiro::Xoshiro512PlusPlus;
use ringlog::*;
use std::error::Error;
use std::net::SocketAddr;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;
use tokio::net::TcpStream;
use tokio::runtime::Builder;
use tokio::time::sleep;

static SEQUENCE: AtomicUsize = AtomicUsize::new(0);

const KB: u32 = 1024;
const MB: u32 = 1024 * KB;

#[derive(Clone, ValueEnum, Debug)]
enum Op {
    Get,
    Put,
}

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    #[arg(long)]
    target: Uri,

    #[arg(long, value_enum)]
    op: Op,

    /// Name of the person to greet
    #[arg(long)]
    size: usize,

    /// Number of times to greet
    #[arg(long, default_value_t = 64 * KB)]
    window: u32,

    #[arg(long, default_value_t = 64 * KB)]
    conn_window: u32,

    #[arg(long, default_value_t = 16 * KB)]
    frame: u32,

    #[arg(long, default_value_t = 1)]
    count: usize,

    #[arg(long)]
    cache_name: String,
}

pub fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::parse();

    let token = std::env::var("MOMENTO_API_KEY").unwrap_or_else(|_| {
        eprintln!("environment variable `MOMENTO_API_KEY` is not set");
        std::process::exit(1);
    });

    let rt = Builder::new_multi_thread()
        .worker_threads(4)
        .thread_name("h2perf-worker")
        .thread_stack_size(3 * 1024 * 1024)
        .enable_all()
        .build()?;

    let level = Level::Info;

    let debug_log = if level <= Level::Info {
        LogBuilder::new().format(ringlog::default_format)
    } else {
        LogBuilder::new()
    }
    .output(Box::new(Stderr::new()))
    .log_queue_depth(1024)
    .single_message_size(4096)
    .build()
    .expect("failed to initialize debug log");

    let mut log = MultiLogBuilder::new()
        .level_filter(LevelFilter::Info)
        .default(debug_log)
        .build()
        .start();

    // spawn logging thread
    rt.spawn(async move {
        loop {
            sleep(Duration::from_millis(1)).await;
            let _ = log.flush();
        }
    });

    // Spawn the root task
    rt.block_on(async { client(args, token).await })
}

async fn client(args: Args, token: String) -> Result<(), Box<dyn Error>> {
    // initialize a prng
    let mut rng = Xoshiro512PlusPlus::from_seed(Seed512::default());

    // prepare 100MB of random value
    let len = 100 * MB as usize;
    let mut vbuf = BytesMut::zeroed(len);
    rng.fill_bytes(&mut vbuf[0..len]);

    let vbuf = Arc::new(vbuf.freeze());

    let root_store = RootCertStore {
        roots: webpki_roots::TLS_SERVER_ROOTS.into(),
    };

    let tlsconfig = Arc::new(rustls::ClientConfig::builder()
        .with_root_certificates(root_store)
        .with_no_client_auth());

    let auth = args.target
        .authority()
        .expect("uri has no authority")
        .clone();

    let port = auth.port_u16().unwrap_or(443);

    let addr = tokio::net::lookup_host((auth.host(), port))
        .await?
        .next()
        .expect("dns found no addresses");


    // Establish TCP connection to the server.
    let tcp = TcpStream::connect(addr).await?;

    tcp.set_nodelay(true).expect("failed to set TCP_NODELAY");

    let connector: TlsConnector = TlsConnector::from(tlsconfig.clone());

    let tls = connector
        .connect(ServerName::try_from(auth.host().to_string()).unwrap(), tcp)
        .await
        .unwrap();

    let (h2, connection) = h2::client::Builder::new()
        .initial_window_size(args.window)
        .initial_connection_window_size(args.conn_window)
        .max_frame_size(args.frame)
        .handshake(tls)
        .await?;

    tokio::spawn(async move {
        connection.await.unwrap();
    });

    for _ in 0..args.count {
        let mut h2: h2::client::SendRequest<Bytes> = h2.clone().ready().await?;

        match args.op {
            Op::Get => {
                // Prepare the HTTP request to send to the server.
                let request = Request::builder()
                    .method(Method::GET)
                    .uri(format!("{}/cache/{}?key={}", args.target, args.cache_name, args.size))
                    .header("authorization", token.clone())
                    .body(())
                    .unwrap();

                debug!("Prepared request: {:?}", request);

                let start = Instant::now();

                // Send the request. The second tuple item allows the caller
                // to stream a request body.
                let (response, _) = h2.send_request(request, true).unwrap();

                let request_latency = start.elapsed().as_micros();

                debug!("Request sent. Waiting for response...");

                let (head, mut body) = response.await?.into_parts();

                debug!("Received response: {:?}", head);

                let (rx_bytes, rx_chunks) = if body.is_end_stream() {
                    (0, 0)
                } else {
                    // The `flow_control` handle allows the caller to manage
                    // flow control.
                    //
                    // Whenever data is received, the caller is responsible for
                    // releasing capacity back to the server once it has freed
                    // the data from memory.
                    let mut flow_control = body.flow_control().clone();

                    // release all capacity that we can release
                    let used = flow_control.used_capacity();
                    let _ = flow_control.release_capacity(used);

                    let mut received = 0;
                    let mut chunks = 0;

                    while let Some(chunk) = body.data().await {
                        let chunk = chunk?;
                        debug!("RX: {:?} bytes", chunk.len());

                        received += chunk.len();
                        chunks += 1;

                        // Let the server send more data.
                        let _ = flow_control.release_capacity(chunk.len());
                    }

                    (received, chunks)
                };

                let latency = start.elapsed().as_micros();

                let response_latency = latency - request_latency;

                let tx_bytes = 0;
                let tx_chunks = 0;

                info!("DATA: TX: {tx_bytes} bytes in {tx_chunks} chunks RX: {rx_bytes} bytes in {rx_chunks} chunks");
                info!("LATENCY: REQUEST: {request_latency} us RESPONSE: {response_latency} us TOTAL: {latency} us");
            }
            Op::Put => {
                let sequence = SEQUENCE.fetch_add(1, Ordering::Relaxed);

                let size: usize = args.size;
                let start = sequence % (vbuf.len() - size);
                let end = start + size;

                let value = vbuf.slice(start..end);

                // Prepare the HTTP request to send to the server.
                let request = Request::builder()
                    .method(Method::PUT)
                    .header("authorization", token.clone())
                    .uri(format!("https://{}/cache/{}?key={}&ttl_seconds=900", args.target, args.cache_name, args.size))
                    .body(())
                    .unwrap();

                let start = Instant::now();

                // Send the request. The second tuple item allows the caller
                // to stream a request body.
                let (response, mut stream) = h2.send_request(request, false).unwrap();
                debug!("Request sent. Sending data...");

                let mut idx = 0;
                let mut tx_chunks = 0;

                while idx < value.len() {
                    stream.reserve_capacity(value.len() - idx);
                    let available = stream.capacity();

                    let end = idx + available;

                    if available == 0 {
                        continue;
                    }

                    debug!("TX: {available} bytes");

                    if end >= value.len() {
                        stream
                            .send_data(value.slice(idx..value.len()), true)
                            .unwrap();
                        break;
                    } else {
                        stream.send_data(value.slice(idx..end), false).unwrap();
                        idx = end;
                    }

                    tx_chunks += 1;
                }

                let request_latency = start.elapsed().as_micros();
                debug!(
                    "data transmitted: {size} bytes in {tx_chunks} chunks in: {request_latency} us"
                );

                stream.reserve_capacity(1024);

                debug!("waiting on response...");

                let (head, mut body) = response.await?.into_parts();

                debug!("Received response: {:?}", head);

                let (rx_bytes, rx_chunks) = if body.is_end_stream() {
                    (0, 0)
                } else {
                    // The `flow_control` handle allows the caller to manage
                    // flow control.
                    //
                    // Whenever data is received, the caller is responsible for
                    // releasing capacity back to the server once it has freed
                    // the data from memory.
                    let mut flow_control = body.flow_control().clone();

                    let mut chunks = 0;
                    let mut received = 0;

                    while let Some(chunk) = body.data().await {
                        let chunk = chunk?;

                        chunks += 1;
                        received += chunk.len();

                        debug!("RX: {:?} bytes", chunk.len());

                        // Let the server send more data.
                        let _ = flow_control.release_capacity(chunk.len());
                    }

                    debug!("data received in {chunks} chunks");

                    (received, chunks)
                };

                let latency = start.elapsed().as_micros();
                let response_latency = latency - request_latency;

                let tx_bytes = size;

                info!("DATA: TX: {tx_bytes} bytes in {tx_chunks} chunks RX: {rx_bytes} bytes in {rx_chunks} chunks");
                info!("LATENCY: REQUEST: {request_latency} us RESPONSE: {response_latency} us TOTAL: {latency} us");
            }
        }
    }

    std::thread::sleep(Duration::from_millis(5));

    Ok(())
}
