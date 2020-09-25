#include "sundial_grpc.grpc.pb.h"
#include "sundial_grpc.pb.h"
#include <iostream>
#include <memory>
#include <string>
#include <grpcpp/grpcpp.h>
#include "txn.h"
#include "global.h"
#include "manager.h"
#include "helper.h"
#include "stats.h"
#include "grpc_async_server.h"
#include "txn_table.h"
#include <grpcpp/grpcpp.h>
#include <grpcpp/health_check_service_interface.h>
#include <grpcpp/ext/proto_server_reflection_plugin.h>

using grpc::Server;
using grpc::ServerAsyncResponseWriter;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerCompletionQueue;
using grpc::Status;
using sundial_rpc::SundialRequest;
using sundial_rpc::SundialResponse;
using sundial_rpc::Sundial_GRPC_ASYNC;

SundialAsyncServiceImp::~SundialAsyncServiceImp(){
    server_->Shutdown();
    // Always shutdown the completion queue after the server.
    cq_->Shutdown();
}

void SundialAsyncServiceImp::run(){
    //printf("running async server");
    //get the ip address of the server
    std::istringstream in(ifconfig_string);
    string line;
    uint32_t num_nodes = 0;
    
    string port;
    while (getline (in, line)) {
        if (line[0] == '#')
            continue;
        else {
            if (num_nodes == g_node_id) {
                //size_t pos = line.find(":");
                //assert(pos != string::npos);
                port = line.substr(0, line.length());
                //port = atoi(port_str.c_str());
                //_server_name = line;
                break;
            }
            num_nodes ++;
        }
    }
    port.append(async_port);
    //printf("ip is:%s\n",port.c_str());
    //printf("port address is %s\n",async_port.substr(1,4).c_str());
    ServerBuilder builder;
    // Listen on the given address without any authentication mechanism.
    builder.AddListeningPort(port, grpc::InsecureServerCredentials());
    // Register "service_" as the instance through which we'll communicate with
    // clients. In this case it corresponds to an *asynchronous* service.
    builder.RegisterService(&service_);
    // Get hold of the completion queue used for the asynchronous communication
    // with the gRPC runtime.
    cq_ = builder.AddCompletionQueue();
    // Finally assemble the server.
    server_ = builder.BuildAndStart();
    std::cout << "Server listening on " << port << std::endl<<"\n";
    // Proceed to the server's main loop.
    HandleRpcs();
}

SundialAsyncServiceImp::CallData::CallData(Sundial_GRPC_ASYNC::AsyncService* service, ServerCompletionQueue* cq): 
service_(service), cq_(cq), responder_(&ctx_), status_(CREATE) {
      // Invoke the serving logic right away.
      Proceed();
    }
int processRequest(SundialRequest* request, SundialResponse* response){
    //printf("async server process request\n");
 if (request->request_type() == SundialRequest::SYS_REQ) {
        // At the beginning of run, (g_num_nodes - 1) sync requests are received
        // as global synchronization. At the end of the run, another
        // (g_num_nodes - 1) sync requests are received as the termination
        // synchronization.
        glob_manager->receive_sync_request();
        response->set_response_type( SundialResponse::SYS_RESP );
        return 1;
    }
    uint64_t txn_id = request->txn_id();
    TxnManager * txn_man = txn_table->get_txn(txn_id);
    // If no TxnManager exists for the requesting transaction, create one.
    if (txn_man == NULL) {
        printf("adding txnID=%ld into txn_table\n", txn_id);
        assert( request->request_type() == SundialRequest::READ_REQ );
        txn_man = new TxnManager();
        txn_man->set_txn_id( txn_id );
        txn_table->add_txn( txn_man );
    }
    //txn_man->rpc_semaphore->print();
    // the transaction handles the RPC call
    txn_man->process_remote_request(request, response);
    
    // if the sub-transaction is no longer required, remove from txn_table
    if (response->response_type() == SundialResponse::RESP_ABORT
        || response->response_type() == SundialResponse::PREPARED_OK_RO
        || response->response_type() == SundialResponse::PREPARED_ABORT
        || response->response_type() == SundialResponse::ACK) {
        txn_table->remove_txn( txn_man );
        delete txn_man;
    }
    //printf("txn id is %d\n",txn_id);
    //txn_man->rpc_semaphore->print();
    //printf("txn rpc sem num is %d",txn_man->rpc_semaphore.);
    //txn_man->rpc_semaphore->decr();

    return 1;
}

void SundialAsyncServiceImp::CallData::Proceed(){
    
    if (status_ == CREATE) {
        //printf("process async request status create\n");
        // Make this instance progress to the PROCESS state.
        status_ = PROCESS;

        // As part of the initial CREATE state, we *request* that the system
        // start processing SayHello requests. In this request, "this" acts are
        // the tag uniquely identifying the request (so that different CallData
        // instances can serve different requests concurrently), in this case
        // the memory address of this CallData instance.
        service_->RequestcontactRemote(&ctx_, &request_, &responder_, cq_, cq_,
                                  this);
      } else if (status_ == PROCESS) {
         // printf("process async request status process\n");
        // Spawn a new CallData instance to serve new clients while we process
        // the one for this CallData. The instance will deallocate itself as
        // part of its FINISH state.
        new CallData(service_, cq_);

        // The actual processing.
        int a = processRequest(&request_ , &response_);

        // And we are done! Let the gRPC runtime know we've finished, using the
        // memory address of this instance as the uniquely identifying tag for
        // the event.
        status_ = FINISH;
        responder_.Finish(response_, Status::OK, this);
      } else {
        GPR_ASSERT(status_ == FINISH);
        // Once in the FINISH state, deallocate ourselves (CallData).
        delete this;
      }
}



void SundialAsyncServiceImp::HandleRpcs(){
    //printf("enter handle rpcs\n");
    // Spawn a new CallData instance to serve new clients.
    new CallData(&service_, cq_.get());
    void* tag;  // uniquely identifies a request.
    bool ok;
    while (true) {
        //printf("enter handle loop\n");
      // Block waiting to read the next event from the completion queue. The
      // event is uniquely identified by its tag, which in this case is the
      // memory address of a CallData instance.
      // The return value of Next should always be checked. This return value
      // tells us whether there is any kind of event or cq_ is shutting down.
      GPR_ASSERT(cq_->Next(&tag, &ok));
      GPR_ASSERT(ok);
      static_cast<CallData*>(tag)->Proceed();
    }
}