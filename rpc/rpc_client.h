#pragma once
#include "sundial.pb.h"
#include "sundial.grpc.pb.h"

class TxnManager;

class SundialRPCClient {
public:
    SundialRPCClient();
    void sendRequest(uint64_t node_id, SundialRequest &request, SundialResponse &response);
    void sendRequestAsync(TxnManager * txn, uint64_t node_id,
                          SundialRequest &request, SundialResponse &response);
    void sendRequestDone(RPC * rpc, TxnManager * txn, SundialResponse &response);
private:
    SundialRPC ** _servers;
};
