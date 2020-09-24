#include "global.h"
#include "rpc_client.h"
#include "stats.h"
#include "manager.h"
#include "txn.h"
#include "sundial.pb.h"
#include "sundial.grpc.pb.h"

SundialRPCClient::SundialRPCClient() {
    _servers = new SundialRPC * [g_num_nodes];
    // get server names
    std::istringstream in(ifconfig_string);
    string line;
    uint32_t num_nodes = 0;
    while ( num_nodes < g_num_nodes && getline(in, line) ) {
        if (line[0] == '#')
            continue;
        else {
            string url = line;
            if (num_nodes != g_node_id)
                _servers[num_nodes] = SundialRPC::NewStub(rpc2::NewClientChannel( url ));
            num_nodes ++;
        }
    }
}

void
SundialRPCClient::sendRequest(uint64_t node_id, SundialRequest &request, SundialResponse &response) {
    RPC rpc;
    //printf("[REQ] send to node %ld. type=%s\n", node_id,
    //       SundialRequest::RequestType_Name(request.request_type()).c_str());
    assert(node_id != g_node_id);
    glob_stats->_stats[GET_THD_ID]->_req_msg_count[ request.request_type() ] ++;
    glob_stats->_stats[GET_THD_ID]->_req_msg_size[ request.request_type() ] += request.SpaceUsedLong();
    _servers[node_id]->contactRemote(&rpc, &request, &response, nullptr);
    /*rpc.Wait();*/
    assert( rpc::status::IsOk(rpc) );

    glob_stats->_stats[GET_THD_ID]->_resp_msg_count[ response.response_type() ] ++;
    glob_stats->_stats[GET_THD_ID]->_resp_msg_size[ response.response_type() ] += response.SpaceUsedLong();
}

void
SundialRPCClient::sendRequestAsync(TxnManager * txn, uint64_t node_id,
                                   SundialRequest &request, SundialResponse &response)
{
    RPC * rpc = new RPC();
    assert(node_id != g_node_id);
    glob_stats->_stats[GET_THD_ID]->_req_msg_count[ request.request_type() ] ++;
    glob_stats->_stats[GET_THD_ID]->_req_msg_size[ request.request_type() ] += request.SpaceUsedLong();

    Closure* call_done = NewCallback(this, &SundialRPCClient::sendRequestDone, rpc, txn, response);

    _servers[node_id]->contactRemote(rpc, &request, &response, call_done);
    // assert( rpc::status::IsOk(rpc) );
}


void
SundialRPCClient::sendRequestDone(RPC * rpc, TxnManager * txn, SundialResponse &response)
{
    rpc->CheckSuccess();

    glob_stats->_stats[GET_THD_ID]->_resp_msg_count[ response.response_type() ] ++;
    glob_stats->_stats[GET_THD_ID]->_resp_msg_size[ response.response_type() ] += response.SpaceUsedLong();

    txn->rpc_semaphore->decr();

    delete rpc;
}
