mod storage_service;
mod storage;
mod executor;

use executor::Executor;
use storage::Storage;
use storage_service::storage_service::{GetDataRequest, GetDataResponse, SaveDataRequest, SaveDataResponse};
use protobuf::Message;

use tokio::net::{TcpListener, TcpStream, TcpSocket};
use tokio_util::codec::{Framed, LengthDelimitedCodec};
use tokio_stream::StreamExt;

use leaky_bucket::RateLimiter;
use std::time::Duration;

use futures::sink::SinkExt;

use std::sync::Arc;
use std::io;

use bytes::Bytes;

#[tokio::main]
async fn main() -> Result<(), Box<io::Error>> {
    let addr = std::env::var("SERVICE_URL").unwrap();
    let db_url = std::env::var("DATABASE_URL").unwrap();
    let rate_s = std::env::var("SERVICE_RATE_LIMIT").unwrap();

    const DEFAULT_RATE: usize = 1000;
    let mut rate = DEFAULT_RATE;

    match rate_s.parse::<usize>() {
        Ok(r) => { rate = r; }
        _ => {}
    }

    println!("SERVICE_URL: {}", &addr);
    println!("DATABASE_URL: {}", &db_url);
    println!("SERVICE_RATE_LIMIT: {}", &rate);

    let rate_limiter = RateLimiter::builder()
        .max(rate)
        .initial(0)
        .build();
    let rate_limiter = Arc::new(rate_limiter);

    let db = Storage::new(&db_url).await.unwrap();
    let executor = Arc::new(Executor::new(db));

    // Creating tcp listener for new connections
    let listener = TcpListener::bind(&addr).await?;
    println!("Listening on: {}", addr);

    // Main request processing
    loop {
        let (socket, _) = listener.accept().await.unwrap();

        tokio::spawn(handle_socket(socket, executor.clone(), rate_limiter.clone()));
    };
}

/// Coroutine for handling each connection over tcp
///
/// It uses data framing to achieve serialization/deserialization correctness
/// of data exchanging.
///
/// Takes connection TcpStream and executor to handle requests.
async fn handle_socket(stream: TcpStream, executor: Arc<Executor>, rate_limiter: Arc<RateLimiter>) -> Result<(), io::Error> {
    // Using custom codec for data framing during communication over socket
    let codec = LengthDelimitedCodec::new();

    let mut framed_stream = Framed::new(stream, codec);

    loop {
        rate_limiter.acquire_one().await;

        let ex = Arc::clone(&executor);

        match framed_stream.next().await {
            Some(next_frame) => {
                match next_frame {
                    Ok(bytes) => {
                        println!("Server: processing incoming frame, len {}", bytes.len());

                        let req_r = parse_request::<SaveDataRequest>(&bytes);
                        if req_r.is_ok() {
                            let mut save_req = req_r.unwrap();
                            match process_save_request(&mut save_req, &mut framed_stream, ex.clone()).await {
                                Ok(()) => { continue; }
                                Err(_) => {}
                            }
                        }

                        let req_r = parse_request::<GetDataRequest>(&bytes);
                        if req_r.is_ok() {
                            let mut get_req = req_r.unwrap();
                            match process_get_request(&mut get_req, &mut framed_stream, ex.clone()).await {
                                Ok(()) => { continue; }
                                Err(_) => {}
                            }
                        }
                    }
                    Err(err) => {
                        break Err(io::Error::new(io::ErrorKind::Other, format!("Server: failed to get next chunk of data, err={}", err)))
                    }
                }
            }
            None => {}
        }
    }
}

async fn process_save_request(req: &mut SaveDataRequest, framed_stream: &mut Framed<TcpStream, LengthDelimitedCodec>, ex: Arc<Executor>) -> Result<(), io::Error> {
    if req.has_key() && req.has_data() {
        println!("Server: handling save request, key {}, data {}", req.key(), req.data());

        let res_bytes = Bytes::from(
            handle_save_request(req, ex).await
        );

        println!("Server: response is ready to be send");

        match framed_stream.send(res_bytes).await {
            Ok(()) => { return Ok(()); }
            Err(_) => {
                return Err(io::Error::new(io::ErrorKind::Other, "Server: failed to send response back"));
            }
        }
    }

    Err(io::Error::new(io::ErrorKind::Other, "Server: save request doesn't contain key or data"))
}

async fn process_get_request(req: &mut GetDataRequest, framed_stream: &mut Framed<TcpStream, LengthDelimitedCodec>, ex: Arc<Executor>) -> Result<(), io::Error> {
    if req.has_key() {
        println!("Server: handling get request, key {}", req.key());

        let res_bytes = Bytes::from(
            handle_get_request(req, ex).await
        );

        println!("Server: response is ready to be send");

        match framed_stream.send(res_bytes).await {
            Ok(()) => { return Ok(()); }
            Err(_) => {
                return Err(io::Error::new(io::ErrorKind::Other, "Server: failed to send response back"));
            }
        }
    }

    Err(io::Error::new(io::ErrorKind::Other, "Server: get request doesn't contain key"))
}

async fn handle_get_request(req: &mut GetDataRequest, executor: Arc<Executor>) -> Vec<u8> {
    let mut response = GetDataResponse::new();

    match executor.get_data_by_key(req.take_key()).await {
        Ok(res) => {
            response.data = std::option::Option::from(res.join("\n").to_string());
        }
        Err(err) => {
            response.err_msg = std::option::Option::from(format!("get request failed due to the following error: {}", err));
        }
    }

    let res_bytes = response.write_to_bytes().unwrap();

    res_bytes
}

async fn handle_save_request(req: &mut SaveDataRequest, executor: Arc<Executor>) -> Vec<u8> {
    let mut response = SaveDataResponse::new();

    match executor.store_data_by_key(req.take_key(), req.take_data()).await {
        Ok(_) => {}
        Err(err) => {
            response.err_msg = std::option::Option::from(format!("save request failed due to the following error: {}", err));
        }
    }

    let res_bytes = encode_response(&response);

    res_bytes
}

fn parse_request<T: Message + std::default::Default>(buf: &[u8]) -> Result<T, protobuf::Error> {
    T::parse_from_bytes(buf)
}

fn encode_response<T: Message + std::default::Default>(resp: &T) -> Vec<u8> {
    let res = resp.write_to_bytes().unwrap();

    res
}

/// Utiliraty coroutine just to check that communication works
async fn client_example(addr: String) {
    let client_socket = TcpSocket::new_v4().unwrap();
    let client_stream = client_socket
    .connect(addr.parse().unwrap())
    .await
    .expect("Client: failed to establish connection with the server");

    let codec = LengthDelimitedCodec::new();
    let mut framed_stream = Framed::new(client_stream, codec);

    // Sending save request
    let mut save_req = SaveDataRequest::new();

    save_req.key = std::option::Option::from(String::from("movie"));
    save_req.data = std::option::Option::from(String::from("Back to the future"));

    let req_bytes = Bytes::from(save_req.write_to_bytes().unwrap());

    framed_stream.send(req_bytes)
        .await
        .expect("Client: failed to send save request");

    let resp = framed_stream.next().await.unwrap().unwrap();

    let mut save_resp = parse_request::<SaveDataResponse>(&resp).expect("Failed to parse save response");

    println!("Client: received save response from server, err_msg {}", save_resp.take_err_msg());

    // Sending get request
    let mut req = GetDataRequest::new();

    req.key = std::option::Option::from(String::from("movie"));

    let req_bytes = Bytes::from(req.write_to_bytes().unwrap());

    framed_stream.send(req_bytes)
        .await
        .expect("Client: failed to send data");

    let resp = framed_stream.next().await.unwrap().unwrap();

    let mut get_resp = parse_request::<GetDataResponse>(&resp).expect("Failed to parse get response");

    println!("Client: received get response from server, data {}, err_msg {}", get_resp.take_data(), get_resp.take_err_msg())
}


#[test]
fn test_encode_decode() {
    let mut expected = GetDataRequest::new();

    expected.key = std::option::Option::from(String::from("key"));

    let bytes = encode_response(&expected);
    let actual = parse_request::<GetDataRequest>(&bytes).expect("Failed to parse request");

    assert_eq!(actual, expected);
}