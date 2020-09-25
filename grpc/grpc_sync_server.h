//
// Created by libin on 6/29/20.
//

#ifndef SUNDIAL_GRPC_SYNC_SERVER_H
#define SUNDIAL_GRPC_SYNC_SERVER_H

#endif //SUNDIAL_GRPC_SYNC_SERVER_H

#include "sundial_grpc.grpc.pb.h"
#include "sundial_grpc.pb.h"
#include <iostream>
#include <memory>
#include <string>
#include <grpc/grpc.h>
#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>
#include <grpcpp/server_context.h>
#include <grpcpp/security/server_credentials.h>
#include <grpcpp/health_check_service_interface.h>
#include <grpcpp/ext/proto_server_reflection_plugin.h>
//#include "txn.h"
//#include "global.h"
//#include "stats.h"
//#include "helper.h"
using grpc::Channel;
using grpc::ServerContext;
using grpc::Status;
using sundial_rpc::SundialRequest;
using sundial_rpc::SundialResponse;
using sundial_rpc::Sundial_GRPC_SYNC;
using grpc::Server;
using grpc::ServerBuilder;
#ifndef ABC
#define ABC
class SundialServiceImp final : public Sundial_GRPC_SYNC::Service
{
    public:
    Status contactRemote(ServerContext* context, const SundialRequest* request, SundialResponse* response) override;
    void run();

};
#endif
