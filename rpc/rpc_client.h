#pragma once


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
