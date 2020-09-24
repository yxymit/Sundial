#include "txn.h"
#include "global.h"
#include "manager.h"
#include "stats.h"
#include "helper.h"
#include "grpc_sync_server.h"
#include "txn_table.h"


Status SundialServiceImp::contactRemote(ServerContext* context, const SundialRequest* request, SundialResponse* response){
     //printf("server gets request\n");
     
     if (request->request_type() == SundialRequest::SYS_REQ) {
        // printf("get sys request\n");
        // At the beginning of run, (g_num_nodes - 1) sync requests are received
        // as global synchronization. At the end of the run, another
        // (g_num_nodes - 1) sync requests are received as the termination
        // synchronization.
        glob_manager->receive_sync_request(); 
        response->set_response_type( SundialResponse::SYS_RESP );
        return Status::OK;
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
    return Status::OK;
}

//let the server run the certain address
void SundialServiceImp::run(){
    //printf("running sync server");
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
    port.append(sync_port);
    //string server_address("0.0.0.0:5051");
    //printf("ip is:%s\n",port.c_str());
    grpc::EnableDefaultHealthCheckService(true);
    grpc::reflection::InitProtoReflectionServerBuilderPlugin();
    ServerBuilder builder;
    //string a("0.0.0.0:6150");
    // Listen on the given address without any authentication mechanism.
    builder.AddListeningPort(port, grpc::InsecureServerCredentials());
    // Register "service" as the instance through which we'll communicate with
    // clients. In this case it corresponds to an *synchronous* service.
    
    builder.RegisterService(this);
    // Finally assemble the server.
    
    std::unique_ptr<Server> server(builder.BuildAndStart());
    // Wait for the server to shutdown. Note that some other thread must be
    // responsible for shutting down the server for this call to ever return.
    //printf("reached here\n");
    std::cout << "Server listening on " << port << std::endl<<"\n";
    server->Wait();
}


