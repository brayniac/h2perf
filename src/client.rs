use std::time::Duration;
use tokio::time::sleep;
use ringlog::*;
use std::net::SocketAddr;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use rand::RngCore;
use rand::SeedableRng;
use rand_xoshiro::Xoshiro512PlusPlus;
use rand_xoshiro::Seed512;
use bytes::BytesMut;
use std::sync::Arc;
use clap::ValueEnum;
use std::time::Instant;
use clap::Parser;
use bytes::Bytes;
use http::{Request, Method};
use std::error::Error;
use tokio::net::TcpStream;

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
    target: SocketAddr,

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
}

#[tokio::main]
pub async fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::parse();

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
    tokio::spawn(async move {
        loop {
            sleep(Duration::from_millis(1)).await;
            let _ = log.flush();
        }
    });

    // initialize a prng
    let mut rng = Xoshiro512PlusPlus::from_seed(Seed512::default());

    // prepare 100MB of random value
    let len = 100 * MB as usize;
    let mut vbuf = BytesMut::zeroed(len);
    rng.fill_bytes(&mut vbuf[0..len]);

    let vbuf = Arc::new(vbuf.freeze());

    // Establish TCP connection to the server.
    let tcp = TcpStream::connect(args.target).await?;

    let (h2, connection) = h2::client::Builder::new()
        .initial_window_size(args.window)
        .initial_connection_window_size(args.conn_window)
        .max_frame_size(args.frame)
        .handshake(tcp)
        .await?;

    tokio::spawn(async move {
        connection.await.unwrap();
    });



    let mut h2: h2::client::SendRequest<Bytes> = h2.ready().await?;

    match args.op {
        Op::Get => {
            // Prepare the HTTP request to send to the server.
            let request = Request::builder()
                            .method(Method::GET)
                            .uri(format!("https://{}/get?{}", args.target, args.size))
                            .body(())
                            .unwrap();

            let start = Instant::now();

            // Send the request. The second tuple item allows the caller
            // to stream a request body.
            let (response, _) = h2.send_request(request, true).unwrap();

            let (head, mut body) = response.await?.into_parts();

            info!("Received response: {:?}", head);

            // The `flow_control` handle allows the caller to manage
            // flow control.
            //
            // Whenever data is received, the caller is responsible for
            // releasing capacity back to the server once it has freed
            // the data from memory.
            let mut flow_control = body.flow_control().clone();

            let mut received = 0;
            let mut chunks = 0;

            while let Some(chunk) = body.data().await {
                let chunk = chunk?;
                info!("RX: {:?} bytes", chunk.len());

                received += chunk.len();
                chunks += 1;

                // Let the server send more data.
                let _ = flow_control.release_capacity(chunk.len());
            }

            info!("data received: {received} in {chunks} chunks");

            let latency = start.elapsed().as_micros();

            info!("latency: {latency} us");
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
                            .uri(format!("https://{}/put", args.target))
                            .body(())
                            .unwrap();

            let start = Instant::now();

            // Send the request. The second tuple item allows the caller
            // to stream a request body.
            let (response, mut stream) = h2.send_request(request, false).unwrap();

            // stream.send_data(value, true).unwrap();

            let mut idx = 0;
            let mut chunks = 0;

            while idx < value.len() {
                stream.reserve_capacity(value.len() - idx);
                let available = stream.capacity();
                
                let end = idx + available;

                if available == 0 {
                    continue;
                }

                info!("TX: {available} bytes");

                if end >= value.len() {
                    stream.send_data(value.slice(idx..value.len()), true).unwrap();
                    break;
                } else {
                    stream.send_data(value.slice(idx..end), false).unwrap();
                    idx = end;
                }

                chunks += 1;
            }

            info!("data transmitted: {size} bytes in {chunks} chunks");

            let latency = start.elapsed().as_micros();
            info!("transmission took: {latency} us");

            stream.reserve_capacity(0);

            info!("waiting on response...");

            let (head, mut body) = response.await?.into_parts();

            info!("Received response: {:?}", head);

            // The `flow_control` handle allows the caller to manage
            // flow control.
            //
            // Whenever data is received, the caller is responsible for
            // releasing capacity back to the server once it has freed
            // the data from memory.
            let mut flow_control = body.flow_control().clone();

            let mut chunks = 0;

            while let Some(chunk) = body.data().await {
                chunks += 1;
                let chunk = chunk?;
                info!("RX: {:?} bytes", chunk.len());

                // Let the server send more data.
                let _ = flow_control.release_capacity(chunk.len());
            }

            info!("data received in {chunks} chunks");

            let latency = start.elapsed().as_micros();

            info!("latency: {latency} us");
        }
    }

    

    Ok(())
}