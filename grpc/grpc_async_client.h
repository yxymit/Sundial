
#include "sundial_grpc.grpc.pb.h"
#include "sundial_grpc.pb.h"
#include <iostream>
#include <memory>
#include <string>
#include <grpcpp/grpcpp.h>
//#include "txn.h"
//#include "global.h"
//#include "helper.h"
//#include "stats.h"

using grpc::Channel;
using grpc::ClientAsyncResponseReader;
using grpc::ClientContext;
using grpc::CompletionQueue;
using grpc::Status;
using sundial_rpc::SundialRequest;
using sundial_rpc::SundialResponse;
using sundial_rpc::Sundial_GRPC_ASYNC;
 
#ifndef SAC
#define SAC
class TxnManager;
class Sundial_Async_Client{
public:
    Sundial_Async_Client(std::string* channel);
    Status contactRemote(CompletionQueue* cq, TxnManager * txn,uint64_t node_id,SundialRequest& request, SundialResponse** response);
    Status contactRemoteDone(CompletionQueue* cq, TxnManager * txn,uint64_t node_id, SundialResponse* response, int count);
private:
    //std::unique_ptr<Sundial_GRPC_ASYNC::Stub> stub_[8];
    std::unique_ptr<Sundial_GRPC_ASYNC::Stub> stub_;
    // struct for keeping state and data information
    struct AsyncClientCall {
        // Container for the data we expect from the server.
        SundialResponse* reply;

        // Context for the client. It could be used to convey extra information to
        // the server and/or tweak certain RPC behaviors.
        ClientContext context;

        // Storage for the status of the RPC upon completion.
        Status status;


        std::unique_ptr<ClientAsyncResponseReader<SundialResponse>> response_reader;
    };
    //CompletionQueue cq;
};

#endif