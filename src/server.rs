use std::net::SocketAddr;
use clap::Parser;
use std::sync::atomic::Ordering;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;
use rand_xoshiro::rand_core::RngCore;
use rand_xoshiro::Seed512;
use rand_xoshiro::rand_core::SeedableRng;
use rand_xoshiro::Xoshiro512PlusPlus;
use bytes::BytesMut;
use h2::server;
use http::{Response, StatusCode};
use tokio::net::TcpListener;

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
                    .handshake(socket).await.unwrap();
                // Accept all inbound HTTP/2 streams sent over the
                // connection.
                while let Some(request) = h2.accept().await {

                    let vbuf = vbuf.clone();

                    //spawn a new task for each stream
                    tokio::spawn(async move {

                        let (mut request, mut respond) = request.unwrap();
                        println!("Received request: {:?}", request);

                        #[allow(clippy::match_single_binding)]
                        match request.uri().path() {
                            "/get" => {
                                let sequence = SEQUENCE.fetch_add(1, Ordering::Relaxed);

                                let size: usize = request.uri().query().map(|v| v.parse().unwrap_or(1024)).unwrap_or(1024);
                                let start = sequence % (vbuf.len() - size);
                                let end = start + size;

                                let value = vbuf.slice(start..end);

                                // Build a response with no body
                                let response = Response::builder()
                                    .status(StatusCode::OK)
                                    .body(())
                                    .unwrap();

                                // Send the response back to the client
                                let mut response = respond.send_response(response, false)
                                    .unwrap();

                                // Write the value to the client
                                response.send_data(value, true).unwrap();
                            }
                            "/put" => {
                                let mut received = 0;
                                let mut chunks = 0;

                                let body = request.body_mut();

                                // keep receiving chunks of the body and releasing capacity
                                // back to the sender as we receive more content
                                while let Some(data) = body.data().await {
                                    let data = data.unwrap();

                                    println!("RX: {} bytes", data.len());

                                    received += data.len();
                                    chunks += 1;

                                    let _ = body.flow_control().release_capacity(data.len());
                                }

                                println!("total received: {received} in {chunks} chunks");

                                // Build a response with no body
                                let response = Response::builder()
                                    .status(StatusCode::OK)
                                    .body(())
                                    .unwrap();

                                // Send the response back to the client
                                respond.send_response(response, true)
                                    .unwrap();
                            }
                            _ => {
                                // Build a response with no body
                                let response = Response::builder()
                                    .status(StatusCode::OK)
                                    .body(())
                                    .unwrap();

                                // Send the response back to the client
                                respond.send_response(response, true)
                                    .unwrap();
                            }
                        }
                    });
                }

            });
        }
    }
}
