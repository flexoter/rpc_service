# rpc_service
Simple rpc service on Rust

## Building
Contains intermediate build steps in order to compile the grpc service .proto file.

## Running

For this service to run properly it's necessary to setup the following env variables:
 - SOURCE_DIR - should specify where the project is located, e.g. "/src/"
 - SERVICE_URL - an address for rpc service to listen to for requests, e.g. 127.0.0.1:8080
 - DATABASE_URL - database url to connect to, should be in similar format to: postgres://postgres:passwordw@localhost:55000

The server uses rate limiting which can be configures through the SERVICE_RATE_LIMIT env variable.
SERVICE_RPS specifies the maximum number of requests the server should handle in one second.

For testing purposes a client_example coroutine may be used to test server for incoming connections