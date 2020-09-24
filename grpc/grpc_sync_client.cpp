//
// Created by libin on 6/28/20.
//

#include "grpc_sync_client.h"
#include "txn.h"
#include "global.h"
#include "helper.h"
#include "manager.h"
#include "stats.h"

//toDo: add more nodes to it
Sundial_Sync_Client::Sundial_Sync_Client(std::string* channel){
    for(int i=0; i<g_num_nodes;i++){
        if(i==g_node_id)
            continue;
    //stub_[i]=Sundial_GRPC_SYNC::NewStub(channel[i]);
    //stub_=Sundial_GRPC_SYNC::NewStub(channel[i]);
    string server_address=channel[i];
    server_address.append(sync_port);
    printf("sync client is connecting to server %s\n",server_address.c_str());  
    stub_=Sundial_GRPC_SYNC::NewStub(grpc::CreateChannel(
                server_address, grpc::InsecureChannelCredentials()));
    }
    /*string a("123.456.789.111:02345");
    stub_=Sundial_GRPC_SYNC::NewStub(grpc::CreateChannel(
                a, grpc::InsecureChannelCredentials()));*/
    
}

Status
Sundial_Sync_Client::contactRemote(uint64_t node_id, SundialRequest& request, SundialResponse* response){
    //toDo: choose the right stub with node id
    ClientContext context;
    //printf("Client sends request\n");
    //Status status = stub_[node_id]->contactRemote(&context, request, &response);
    glob_stats->_stats[GET_THD_ID]->_req_msg_count[ request.request_type() ] ++;
    glob_stats->_stats[GET_THD_ID]->_req_msg_size[ request.request_type() ] += request.SpaceUsedLong();
    Status status = stub_->contactRemote(&context, request, response);
    if (status.ok()) {
        //printf("status ok\n");
        glob_stats->_stats[GET_THD_ID]->_resp_msg_count[ response->response_type() ] ++;
        glob_stats->_stats[GET_THD_ID]->_resp_msg_size[ response->response_type() ] += response->SpaceUsedLong();
        return status;
    } else {
        //printf("request type is ")
        std::cout << status.error_code() << ": " << status.error_message()
                  << std::endl;\
        //assert(false);          
         string error("failed to connect to all addresses");         
         if(status.error_message().compare(error)==0){
             assert(false);
         }         
         contactRemote(node_id, request, response);         
        //assert(false);
        return status;
    }

}




