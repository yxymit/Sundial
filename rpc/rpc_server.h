#pragma once
#include "sundial.pb.h"
#include "sundial.grpc.pb.h"
class SundialRPCServerImpl : public SundialRPC {
public:
    void run();
    void Export(net_http::HTTPServerInterface* http_server);
    void contactRemote(RPC* rpc, const SundialRequest* request,
                     SundialResponse* response,
                     Closure* done_callback) override;
private:
    ThreadPool *    _thread_pool;
};

