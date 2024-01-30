# rpc_service
Simple rpc service on Rust

## Building
Contains intermediate build steps in order to compile the grpc service .proto file.

For it to run properly it's necessary to setup the following env variables:
 - SOURCE_DIR - should specify where the project is located, e.g. "/src/"
 - SERVICE_URL - an address for rpc service to listen to for requests, e.g. 127.0.0.1:8080
 - DATABASE_URL - database url to connect to, should be in similar format to: postgres://postgres:passwordw@localhost:55000

## Running
For testing purposes a client_example coroutine may be used to test server for incoming connections