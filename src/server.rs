use bytes::BytesMut;
use clap::Parser;
use h2::server;
use http::{Response, StatusCode};
use rand_xoshiro::rand_core::RngCore;
use rand_xoshiro::rand_core::SeedableRng;
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
use tokio::net::TcpListener;
use tokio::runtime::Builder;
use tokio::time::sleep;

static SEQUENCE: AtomicUsize = AtomicUsize::new(0);

const KB: u32 = 1024;
const MB: u32 = 1024 * KB;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    #[arg(long)]
    listen: SocketAddr,

    /// Number of times to greet
    #[arg(long, default_value_t = 64 * KB)]
    window: u32,

    #[arg(long, default_value_t = 64 * KB)]
    conn_window: u32,

    #[arg(long, default_value_t = 16 * KB)]
    frame: u32,
}

#[tokio::main]
pub async fn main() {
    let args = Args::parse();

    let rt = Builder::new_multi_thread()
        .worker_threads(4)
        .thread_name("h2perf-worker")
        .thread_stack_size(3 * 1024 * 1024)
        .enable_all()
        .build()
        .expect("failed to initialize runtime");

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
    rt.spawn(async { server(args).await });

    loop {
        std::thread::sleep(Duration::from_secs(1))
    }
}

async fn server(args: Args) {
    let listener = TcpListener::bind(args.listen).await.unwrap();

    // initialize a prng
    let mut rng = Xoshiro512PlusPlus::from_seed(Seed512::default());

    // prepare 100MB of random value
    let len = 100 * MB as usize;
    let mut vbuf = BytesMut::zeroed(len);
    rng.fill_bytes(&mut vbuf[0..len]);

    let vbuf = Arc::new(vbuf.freeze());

    // Accept all incoming TCP connections.
    loop {
        if let Ok((socket, _peer_addr)) = listener.accept().await {
            let vbuf = vbuf.clone();

            // Spawn a new task to process each connection.
            tokio::spawn(async move {
                // Start the HTTP/2 connection handshake
                let mut h2 = server::Builder::new()
                    .initial_window_size(args.window)
                    .initial_connection_window_size(args.conn_window)
                    .handshake(socket)
                    .await
                    .unwrap();
                // Accept all inbound HTTP/2 streams sent over the
                // connection.
                while let Some(request) = h2.accept().await {
                    let vbuf = vbuf.clone();

                    //spawn a new task for each stream
                    tokio::spawn(async move {
                        let (mut request, mut respond) = request.unwrap();

                        let start = Instant::now();

                        info!("Received request: {:?}", request);

                        #[allow(clippy::match_single_binding)]
                        match request.uri().path() {
                            "/get" => {
                                let body = request.body_mut();

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
                                        let chunk = chunk.unwrap();
                                        debug!("RX: {:?} bytes", chunk.len());

                                        received += chunk.len();
                                        chunks += 1;

                                        // Let the server send more data.
                                        let _ = flow_control.release_capacity(chunk.len());
                                    }

                                    (received, chunks)
                                };

                                let request_latency = start.elapsed().as_micros();

                                // determine response payload

                                let sequence = SEQUENCE.fetch_add(1, Ordering::Relaxed);

                                let size: usize = request
                                    .uri()
                                    .query()
                                    .map(|v| v.parse().unwrap_or(1024))
                                    .unwrap_or(1024);
                                let idx = sequence % (vbuf.len() - size);
                                let end = idx + size;

                                let value = vbuf.slice(idx..end);

                                // Build a response with no body
                                let response =
                                    Response::builder().status(StatusCode::OK).body(()).unwrap();

                                // Send the response back to the client
                                let mut stream = respond.send_response(response, false).unwrap();

                                debug!(
                                    "sent headers... sending data... latency: {} us",
                                    start.elapsed().as_micros()
                                );

                                // Send the data back to the client
                                let mut idx = 0;
                                let mut tx_chunks = 0;
                                let tx_bytes = size;

                                while idx < value.len() {
                                    stream.reserve_capacity(value.len() - idx);

                                    let mut available = stream.capacity();

                                    // default minimum of a 16KB frame...
                                    if available == 0 {
                                        available = 16384;
                                    }

                                    debug!("TX: {:?} bytes", available);

                                    let end = idx + available;

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

                                let latency = start.elapsed().as_micros();
                                let response_latency = latency - request_latency;

                                info!("DATA: TX: {tx_bytes} bytes in {tx_chunks} chunks RX: {rx_bytes} bytes in {rx_chunks} chunks");
                                info!("LATENCY: REQUEST: {request_latency} us RESPONSE: {response_latency} us TOTAL: {latency} us");
                            }
                            "/put" => {
                                let body = request.body_mut();

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
                                        let chunk = chunk.unwrap();
                                        debug!("RX: {:?} bytes", chunk.len());

                                        received += chunk.len();
                                        chunks += 1;

                                        // Let the server send more data.
                                        let _ = flow_control.release_capacity(chunk.len());
                                    }

                                    (received, chunks)
                                };

                                let request_latency = start.elapsed().as_micros();

                                // Build a response with no body
                                let response =
                                    Response::builder().status(StatusCode::OK).body(()).unwrap();

                                // Send the response back to the client
                                respond.send_response(response, true).unwrap();

                                let tx_bytes = 0;
                                let tx_chunks = 0;

                                let latency = start.elapsed().as_micros();
                                let response_latency = latency - request_latency;

                                info!("DATA: TX: {tx_bytes} bytes in {tx_chunks} chunks RX: {rx_bytes} bytes in {rx_chunks} chunks");
                                info!("LATENCY: REQUEST: {request_latency} us RESPONSE: {response_latency} us TOTAL: {latency} us");
                            }
                            _ => {
                                // Build a response with no body
                                let response =
                                    Response::builder().status(StatusCode::OK).body(()).unwrap();

                                // Send the response back to the client
                                respond.send_response(response, true).unwrap();
                            }
                        }
                    });
                }
            });
        }
    }
}
