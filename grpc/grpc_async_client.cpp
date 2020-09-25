#include "sundial_grpc.grpc.pb.h"
#include "sundial_grpc.pb.h"
#include <iostream>
#include <memory>
#include <string>
#include <grpcpp/grpcpp.h>
#include "txn.h"
#include "global.h"
#include "helper.h"
#include "grpc_async_client.h"
#include "txn.h"
#include "helper.h"
#include "manager.h"
#include "stats.h"

using grpc::Channel;
using grpc::ClientAsyncResponseReader;
using grpc::ClientContext;
using grpc::CompletionQueue;
using grpc::Status;
using sundial_rpc::SundialRequest;
using sundial_rpc::SundialResponse;
using sundial_rpc::Sundial_GRPC_ASYNC;

Sundial_Async_Client::Sundial_Async_Client(std::string* channel){
  for(int i=0; i<g_num_nodes;i++){
        if(i==g_node_id)
            continue;
     std::string server_address = channel[i];
     server_address.append(async_port);
     printf("async client is connecting to server %s\n",server_address.c_str());       
    //stub_[i]=Sundial_GRPC_ASYNC::NewStub(grpc::CreateChannel(
    //            server_address, grpc::InsecureChannelCredentials()));
    stub_=Sundial_GRPC_ASYNC::NewStub(grpc::CreateChannel(
                server_address, grpc::InsecureChannelCredentials()));
    }
    //stub_=Sundial_GRPC_ASYNC::NewStub(channel);

};

//toDo: more than 2 nodes
Status Sundial_Async_Client:: contactRemote(CompletionQueue* cq, TxnManager * txn,uint64_t node_id,SundialRequest& request, SundialResponse** response){

        // Call object to store rpc data
        AsyncClientCall* call = new AsyncClientCall;

        // stub_->PrepareAsyncSayHello() creates an RPC object, returning
        // an instance to store in "call" but does not actually start the RPC
        // Because we are using the asynchronous API, we need to hold on to
        // the "call" instance in order to get updates on the ongoing RPC.
        call->response_reader =
            stub_->PrepareAsynccontactRemote(&call->context, request, cq);

        // StartCall initiates the RPC call
        call->response_reader->StartCall();
        call->reply=*response;
        // Request that, upon completion of the RPC, "reply" be updated with the
        // server's response; "status" with the indication of whether the operation
        // was successful. Tag the request with the memory address of the call object.
        call->response_reader->Finish(call->reply, &call->status, (void*)call);
        glob_stats->_stats[GET_THD_ID]->_req_msg_count[ request.request_type() ] ++;
        glob_stats->_stats[GET_THD_ID]->_req_msg_size[ request.request_type() ] += request.SpaceUsedLong();
    return Status::OK;
}

Status Sundial_Async_Client::contactRemoteDone(CompletionQueue* cq, TxnManager * txn,uint64_t node_id, SundialResponse* response, int count){
    if(count==0){
        return Status::OK;
    }
    void* got_tag;
        bool ok = false;
        int local_count=0;
        // Block until the next result is available in the completion queue "cq".
        while (cq->Next(&got_tag, &ok)) {
            local_count++;
            // The tag in this example is the memory location of the call object
            AsyncClientCall* call = static_cast<AsyncClientCall*>(got_tag);

            // Verify that the request was completed successfully. Note that "ok"
            // corresponds solely to the request for updates introduced by Finish().
            GPR_ASSERT(ok);


            // Once we're complete, deallocate the call object.
             //doing the cleaning
    glob_stats->_stats[GET_THD_ID]->_resp_msg_count[ call->reply->response_type() ] ++;
    glob_stats->_stats[GET_THD_ID]->_resp_msg_size[ call->reply->response_type() ] += call->reply->SpaceUsedLong();
            delete call;
            if(local_count==count){
            break;
        }
        }
        
     
    //txn->rpc_semaphore->decr();
      return Status::OK;
   
}