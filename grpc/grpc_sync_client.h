//
// Created by libin on 6/28/20.
//

#ifndef SUNDIAL_GRPC_CLIENT_H
#define SUNDIAL_GRPC_CLIENT_H

#endif //SUNDIAL_GRPC_CLIENT_H
#include "sundial_grpc.grpc.pb.h"
#include "sundial_grpc.pb.h"
#include <iostream>
#include <memory>
#include <string>
#include <grpc/grpc.h>
#include <grpcpp/grpcpp.h>
#include <grpcpp/channel.h>
#include <grpcpp/client_context.h>
#include <grpcpp/create_channel.h>
#include <grpcpp/security/credentials.h>

//#include "txn.h"
//#include "global.h"
//#include "helper.h"
//#include "stats.h"
using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;
using sundial_rpc::SundialRequest;
using sundial_rpc::SundialResponse;
using sundial_rpc::Sundial_GRPC_SYNC;
//toDo: now assume we only have 2 nodes
#ifndef SSC
#define SSC
class Sundial_Sync_Client{
public:
    Sundial_Sync_Client(std::string* channels);
    Status contactRemote(uint64_t node_id,SundialRequest& request, SundialResponse* response);
private:
    //std::unique_ptr<Sundial_GRPC_SYNC::Stub> stub_[4];
    std::unique_ptr<Sundial_GRPC_SYNC::Stub> stub_;
};
#endif