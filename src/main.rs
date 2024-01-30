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

use futures::sink::SinkExt;

use std::sync::Arc;
use std::io;

use bytes::Bytes;

#[tokio::main]
async fn main() -> Result<(), Box<io::Error>> {
    let addr = std::env::var("SERVICE_URL").unwrap();
    let db_url = std::env::var("DATABASE_URL").unwrap();

    let db = Storage::new(&db_url).await.unwrap();
    let executor = Arc::new(Executor::new(db));

    // Creating tcp listener for new connections
    let listener = TcpListener::bind(&addr).await?;
    println!("Listening on: {}", addr);

    let client_socket = TcpSocket::new_v4().unwrap();

    tokio::spawn(async move {
        let client_stream = client_socket
        .connect(addr.parse().unwrap())
        .await
        .expect("Client: failed to establish connection with the server");

        let codec = LengthDelimitedCodec::new();
        let mut framed_stream = Framed::new(client_stream, codec);

        let mut save_req = SaveDataRequest::new();

        save_req.key = String::from("movie");
        save_req.data = String::from("Back to the future");

        let req_bytes = Bytes::from(save_req.write_to_bytes().unwrap());

        framed_stream.send(req_bytes).await.expect("Client: failed to send save request");

        let resp = framed_stream.next().await.unwrap().unwrap();

        let save_resp = parse_request::<SaveDataResponse>(&resp).expect("Failed to parse save response");

        println!("Client: received save response from server, err_msg {}", save_resp.err_msg);

        let mut req = GetDataRequest::new();

        req.key = String::from("movie");

        let req_bytes = Bytes::from(req.write_to_bytes().unwrap());

        framed_stream.send(req_bytes).await.expect("Client: failed to send data");

        let resp = framed_stream.next().await.unwrap().unwrap();

        let get_resp = parse_request::<GetDataResponse>(&resp).expect("Failed to parse get response");

        println!("Client: received get response from server, data {}, err_msg {}", get_resp.data, get_resp.err_msg)
    });

    // Main request processing
    loop {
        let (socket, _) = listener.accept().await.unwrap();

        tokio::spawn(handle_socket(socket, executor.clone()));
    };
}

async fn handle_socket(stream: TcpStream, executor: Arc<Executor>) -> Result<(), io::Error> {
    let codec = LengthDelimitedCodec::new();

    let mut framed_stream = Framed::new(stream, codec);

    loop {
        let ex = Arc::clone(&executor);

        match framed_stream.next().await {
            Some(next_frame) => {
                let mut res_bytes = None;

                match next_frame {
                    Ok(bytes) => {
                        println!("Server: processing incoming frame, len {}", bytes.len());

                        if let Ok(get_req) = parse_request::<GetDataRequest>(&bytes) {
                            res_bytes = Some(Bytes::from(
                                handle_get_request(get_req, ex).await
                            ));
                        } else if let Ok(save_req) = parse_request::<SaveDataRequest>(&bytes) {
                            res_bytes = Some(Bytes::from(
                                handle_save_request(save_req, ex).await
                            ));
                        }

                        if res_bytes.is_none() {
                            return Err(io::Error::new(io::ErrorKind::Other, "Server: received unknown request type"));
                        }

                        match framed_stream.send(res_bytes.unwrap()).await {
                            Ok(()) => { }
                            Err(_) => {
                                break Err(io::Error::new(io::ErrorKind::Other, "Server: failed to send response back"))
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

async fn handle_get_request(req: GetDataRequest, executor: Arc<Executor>) -> Vec<u8> {
    let mut response = GetDataResponse::new();

    match executor.get_data_by_key(req.key).await {
        Ok(res) => {
            response.data = res.join("\n");
            response.err_msg = "".to_string();
        }
        Err(err) => {
            response.data = "".to_string();
            response.err_msg = format!("get request failed due to the following error: {}", err);
        }
    }

    let res_bytes = response.write_to_bytes().unwrap();

    res_bytes
}

async fn handle_save_request(req: SaveDataRequest, executor: Arc<Executor>) -> Vec<u8> {
    let mut response = SaveDataResponse::new();

    match executor.store_data_by_key(req.key, req.data).await {
        Ok(_) => {
            response.err_msg = "".to_string();
        }
        Err(err) => {
            response.err_msg = format!("save request failed due to the following error: {}", err);
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

#[test]
fn test_encode_decode() {
    let mut expected = GetDataRequest::new();

    expected.key = String::from("key");

    let bytes = encode_response(&expected);
    let actual = parse_request::<GetDataRequest>(&bytes).expect("Failed to parse request");

    assert_eq!(actual, expected);
}