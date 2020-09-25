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

using grpc::Server;
using grpc::ServerAsyncResponseWriter;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerCompletionQueue;
using grpc::Status;
using sundial_rpc::SundialRequest;
using sundial_rpc::SundialResponse;
using sundial_rpc::Sundial_GRPC_ASYNC;

#ifndef SAS
#define SAS

class SundialAsyncServiceImp {
    public:
    ~SundialAsyncServiceImp();
    //Status contactRemote(ServerContext* context, const SundialRequest* request, SundialResponse* response) ;
    void run();
    private:
    // Class encompasing the state and logic needed to serve a request.
  class CallData {
   public:
    // Take in the "service" instance (in this case representing an asynchronous
    // server) and the completion queue "cq" used for asynchronous communication
    // with the gRPC runtime.
    CallData(Sundial_GRPC_ASYNC::AsyncService* service, ServerCompletionQueue* cq);

    void Proceed();

   private:
    // The means of communication with the gRPC runtime for an asynchronous
    // server.
    Sundial_GRPC_ASYNC::AsyncService* service_;
    // The producer-consumer queue where for asynchronous server notifications.
    ServerCompletionQueue* cq_;
    // Context for the rpc, allowing to tweak aspects of it such as the use
    // of compression, authentication, as well as to send metadata back to the
    // client.
    ServerContext ctx_;

    // What we get from the client.
    SundialRequest request_;
    // What we send back to the client.
    SundialResponse response_;

    // The means to get back to the client.
    ServerAsyncResponseWriter<SundialResponse> responder_;

    // Let's implement a tiny state machine with the following states.
    enum CallStatus { CREATE, PROCESS, FINISH };
    CallStatus status_;  // The current serving state.
  };
    void HandleRpcs();
    std::unique_ptr<ServerCompletionQueue> cq_;
    Sundial_GRPC_ASYNC::AsyncService service_;
    std::unique_ptr<Server> server_;

    
};

#endif