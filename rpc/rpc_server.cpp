#include "rpc_server.h"
#include "global.h"
#include "txn.h"
#include "txn_table.h"
#include "manager.h"
#include "sundial.pb.h"
#include "sundial.grpc.pb.h"

void
SundialRPCServerImpl::run() {
    // Configure the options for your HTTPServer2 instance.
    auto options = absl::make_unique<HTTPServer2::EventModeOptions>();
    options->SetVersion("SundialRPC");
    options->SetServerType("sundial_rpc_server");

    std::istringstream in(ifconfig_string);
    string line;
    uint32_t num_nodes = 0;
    uint64_t port = 0;
    while (getline (in, line)) {
        if (line[0] == '#')
            continue;
        else {
            if (num_nodes == g_node_id) {
                size_t pos = line.find(":");
                assert(pos != string::npos);
                string port_str = line.substr(pos + 1, line.length());
                port = atoi(port_str.c_str());
                //_server_name = line;
                break;
            }
            num_nodes ++;
        }
    }

    options->AddPort( port );

    // Create the HTTPServer2 instance. Use the process-wide default
    // EventManager, which is also used by Stubby2 (by default).
    std::unique_ptr<HTTPServer2> http_server(
        HTTPServer2::CreateEventDrivenModeServer(
            EventManager::DefaultEventManager(), std::move(options))
        .ValueOrDie());

    // 4. Export service to make it available.
    Export(http_server.get());

    // 5. Set up your server to start accepting requests.
    CHECK_OK(http_server->StartAcceptingRequests());

    // 6. Keep your program running until your server shuts down.
    http_server->WaitForTermination();
}

void
SundialRPCServerImpl::Export(net_http::HTTPServerInterface* http_server) {
    ExportServiceTo(http_server);

    _thread_pool = new ThreadPool(NUM_RPC_SERVER_THREADS);
    _thread_pool->StartWorkers();
    SetThreadPool("contactRemote", _thread_pool);
}

void
SundialRPCServerImpl::contactRemote(RPC* rpc, const SundialRequest* request,
                     SundialResponse* response,
                     Closure* done_callback) {
    // Calls done_callback->Run() when it goes out of scope.
    AutoClosureRunner done_runner(done_callback);

    if (request->request_type() == SundialRequest::SYS_REQ) {
        // At the beginning of run, (g_num_nodes - 1) sync requests are received
        // as global synchronization. At the end of the run, another
        // (g_num_nodes - 1) sync requests are received as the termination
        // synchronization.
        glob_manager->receive_sync_request();
        response->set_response_type( SundialResponse::SYS_RESP );
        return;
    }

    uint64_t txn_id = request->txn_id();
    TxnManager * txn_man = txn_table->get_txn(txn_id);
    // If no TxnManager exists for the requesting transaction, create one.
    if (txn_man == NULL) {
        //printf("adding txnID=%ld into txn_table\n", txn_id);
        assert( request->request_type() == SundialRequest::READ_REQ );
        txn_man = new TxnManager();
        txn_man->set_txn_id( txn_id );
        txn_table->add_txn( txn_man );
    }
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
}

