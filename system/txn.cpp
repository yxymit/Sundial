#include "txn.h"
#include "row.h"
#include "workload.h"
#include "ycsb.h"
#include "worker_thread.h"
#include "table.h"
#include "catalog.h"
#include "index_btree.h"
#include "index_hash.h"
#include "helper.h"
#include "manager.h"
#include "query.h"
#include "txn_table.h"
#include "cc_manager.h"
#include "store_procedure.h"
#include "ycsb_store_procedure.h"
#include "tpcc_store_procedure.h"
#include "tpcc_query.h"
#include "tpcc_helper.h"
#include "grpc_async_server.h"
#include "grpc_sync_server.h"
#include "grpc_async_client.h"
#include "grpc_sync_client.h"
#include "tictoc_manager.h"
#include "lock_manager.h"
#include "f1_manager.h"
#include "sundial_grpc.grpc.pb.h"
#include "sundial_grpc.pb.h"
#if CC_ALG == NO_WAIT || CC_ALG == WAIT_DIE
#include "row_lock.h"
#endif
#include "log.h"

// TODO. cleanup the accesses related malloc code.

TxnManager::TxnManager(QueryBase * query, WorkerThread * thread)
{
    _store_procedure = GET_WORKLOAD->create_store_procedure(this, query);
    _cc_manager = CCManager::create(this);
    _txn_state = RUNNING;
    _worker_thread = thread;

    _txn_start_time = get_sys_clock();
    _txn_restart_time = _txn_start_time;
    _lock_wait_time = 0;
    _net_wait_time = 0;

    _is_sub_txn = false;
    _is_single_partition = true;
    _is_read_only = true;
    _is_remote_abort = false;

    log_semaphore = new SemaphoreSync();
    dependency_semaphore = new SemaphoreSync();
    rpc_semaphore = new SemaphoreSync();
}

TxnManager::~TxnManager()
{
    if (_store_procedure)
        delete _store_procedure;
    delete _cc_manager;
    for (auto kvp : _remote_nodes_involved)
        delete kvp.second;
    delete log_semaphore;
    delete dependency_semaphore;
    delete rpc_semaphore;
}

void
TxnManager::update_stats()
{
    _finish_time = get_sys_clock();
    // TODO. collect stats for sub_queries.
    if (is_sub_txn())
        return;

#if WORKLOAD == TPCC && STATS_ENABLE
    uint32_t type = ((QueryTPCC *)_store_procedure->get_query())->type;
    if (_txn_state == COMMITTED) {
        glob_stats->_stats[GET_THD_ID]->_commits_per_txn_type[ type ]++;
        glob_stats->_stats[GET_THD_ID]->_time_per_txn_type[ type ] +=
            _finish_time - _txn_start_time - _lock_wait_time - _net_wait_time;
    } else
        glob_stats->_stats[GET_THD_ID]->_aborts_per_txn_type[ type ]++;
#endif

    if ( _txn_state == COMMITTED ) {
        INC_INT_STATS(num_commits, 1);
        uint64_t latency;
        if (is_single_partition()) {
            INC_FLOAT_STATS(single_part_execute_phase, _commit_start_time - _txn_restart_time);
        #if CONTROLLED_LOCK_VIOLATION
            INC_FLOAT_STATS(single_part_precommit_phase, _precommit_finish_time - _commit_start_time);
        #endif
        #if LOG_ENABLE
            INC_FLOAT_STATS(single_part_log_latency, _log_ready_time - _commit_start_time);
        #endif
            INC_FLOAT_STATS(single_part_commit_phase, _finish_time - _commit_start_time);
            INC_FLOAT_STATS(single_part_abort, _txn_restart_time - _txn_start_time);

            INC_INT_STATS(num_single_part_txn, 1);
            latency = _finish_time - _txn_start_time;
        } else {
            INC_FLOAT_STATS(multi_part_execute_phase, _prepare_start_time - _txn_restart_time);
        #if CONTROLLED_LOCK_VIOLATION
            INC_FLOAT_STATS(multi_part_precommit_phase, _precommit_finish_time - _prepare_start_time);
        #endif
            INC_FLOAT_STATS(multi_part_prepare_phase, _commit_start_time - _prepare_start_time);
            INC_FLOAT_STATS(multi_part_commit_phase, _finish_time - _commit_start_time);
            INC_FLOAT_STATS(multi_part_abort, _txn_restart_time - _txn_start_time);

            INC_INT_STATS(num_multi_part_txn, 1);
            latency = _commit_start_time - _txn_start_time;
        }
#if COLLECT_LATENCY
        INC_FLOAT_STATS(txn_latency, latency);
        vector<double> &all = glob_stats->_stats[GET_THD_ID]->all_latency;
        all.push_back(latency);
#endif
    } else if ( _txn_state == ABORTED ) {
        INC_INT_STATS(num_aborts, 1);
        if (_store_procedure->is_self_abort()) {
            INC_INT_STATS(num_aborts_terminate, 1);
        } else {
            INC_INT_STATS(num_aborts_restart, 1);
        }
        if (_is_remote_abort) {
            INC_INT_STATS(num_aborts_remote, 1);
        } else {
            INC_INT_STATS(num_aborts_local, 1);
        }
    } else
        assert(false);
}

RC
TxnManager::restart() {
    assert(_txn_state == ABORTED);
    _is_single_partition = true;
    _is_read_only = true;
    _is_remote_abort = false;
    _txn_restart_time = get_sys_clock();
    _store_procedure->init();
    for (auto kvp : _remote_nodes_involved)
        delete kvp.second;
    _remote_nodes_involved.clear();
    return start();
}

RC
TxnManager::start()
{
    RC rc = RCOK;
    _txn_state = RUNNING;
    // running transaction on the host node
    rc = _store_procedure->execute();
    assert(rc == COMMIT || rc == ABORT);
    // Handle single-partition transactions
    if (is_single_partition()) {
        _commit_start_time = get_sys_clock();
        rc = process_commit_phase_singlepart(rc);
    } else {
        if (rc == ABORT){
            CompletionQueue* cq = new CompletionQueue();
            rc = process_2pc_phase2(ABORT,cq);
            delete cq;
            }
        else {
            _prepare_start_time = get_sys_clock();
            CompletionQueue* cq = new CompletionQueue();
            process_2pc_phase1(cq);
             delete cq;
            _commit_start_time = get_sys_clock();
            CompletionQueue* cq2 = new CompletionQueue();
            rc = process_2pc_phase2(COMMIT,cq2);
            delete cq2;
        }
    }
    update_stats();
    return rc;
}

RC
TxnManager::process_commit_phase_singlepart(RC rc)
{
    if (rc == COMMIT) {
        _txn_state = COMMITTING;
    } else if (rc == ABORT) {
        _txn_state = ABORTING;
        _store_procedure->txn_abort();
    } else
        assert(false);
#if LOG_ENABLE
    // TODO. Changed from design A to design B
    // [Design A] the worker thread is detached from the transaction once the log
    // buffer is filled. The logging thread handles the rest of the commit.
    // [Design B] the worker thread sleeps until logging finishes and handles the
    // rest of the commit itself.
    // Design B is simpler than A for 2PC. Since it is hard to detach a
    // transaction from an RPC thread during an RPC call.
    // TODO Need to carefully test performance to make sure design B is not
    // slower than design A.
    if (rc == ABORT) {
        _cc_manager->cleanup(rc);
        _txn_state = ABORTED;// Spawn a new CallData instance to serve new clients.
    
        rc = ABORT;
    } else { // rc == COMMIT
        char * log_record = NULL;
        uint32_t log_record_size = _cc_manager->get_log_record(log_record);
        if (log_record_size > 0) {
            assert(log_record);
            log_semaphore->incr();
            //log_manager->log(this, log_record_size, log_record);
            delete [] log_record;
            // The worker thread will be waken up by the logging thread after
            // the logging operation finishes.
        }
  #if CONTROLLED_LOCK_VIOLATION
        //INC_INT_STATS(num_precommits, 1);
        _cc_manager->process_precommit_phase_coord();
  #endif
        _precommit_finish_time = get_sys_clock();
  #if ENABLE_ADMISSION_CONTROL
        // now the transaction has precommitted, the current thread is inactive,
        // need to increase the quota of another thread.
        //uint64_t wakeup_thread_id = glob_manager->next_wakeup_thread() % g_num_worker_threads;
        //glob_manager->get_worker_thread( wakeup_thread_id )->incr_quota();
        glob_manager->wakeup_next_thread();
  #endif
        // For read-write transactions, this waits for logging to complete.
        // For read-only transactions, this waits for dependent transactions to
        // commit (CLV only).
        uint64_t tt = get_sys_clock();

        //log_semaphore->wait();

        _log_ready_time = get_sys_clock();
        INC_FLOAT_STATS(log_ready_time, get_sys_clock() - tt);

        dependency_semaphore->wait();
        INC_FLOAT_STATS(dependency_ready_time, get_sys_clock() - tt);

        rc = COMMIT;
        _cc_manager->cleanup(rc);
        _txn_state = COMMITTED;
    }
#else
    // if logging didn't happen, process commit phase
    _cc_manager->cleanup(rc);
    _txn_state = (rc == COMMIT)? COMMITTED : ABORTED;
#endif
    return rc;
}

// For Distributed DBMS
// ====================
/*
RC
TxnManager::send_remote_read_request(uint64_t node_id, uint64_t key, uint64_t index_id,
                                     uint64_t table_id, access_t access_type)
{
    _is_single_partition = false;
    if ( _remote_nodes_involved.find(node_id) == _remote_nodes_involved.end() ) {
        _remote_nodes_involved[node_id] = new RemoteNodeInfo;
        _remote_nodes_involved[node_id]->state = RUNNING;
    }

    SundialRequest &request = _remote_nodes_involved[node_id]->request;
    SundialResponse &response = _remote_nodes_involved[node_id]->response;
    request.Clear();
    response.Clear();
    request.set_txn_id( get_txn_id() );
    request.set_request_type( SundialRequest::READ_REQ );

    SundialRequest::ReadRequest * read_request = request.add_read_requests();
    read_request->set_key(key);
    read_request->set_index_id(index_id);
    read_request->set_access_type(access_type);

    rpc_client->sendRequest(node_id, request, response);

    // handle RPC response
    assert(response.response_type() == SundialResponse::RESP_OK
           || response.response_type() ==  SundialResponse::RESP_ABORT);
    if (response.response_type() == SundialResponse::RESP_OK) {
        ((LockManager *)_cc_manager)->process_remote_read_response(node_id, access_type, response);
        return RCOK;
    } else {
        _remote_nodes_involved[node_id]->state = ABORTED;
        _is_remote_abort = true;
        return ABORT;
    }
}
*/
RC
TxnManager::send_remote_read_request(uint64_t node_id, uint64_t key, uint64_t index_id,
                                     uint64_t table_id, access_t access_type)
{
    _is_single_partition = false;
    if ( _remote_nodes_involved.find(node_id) == _remote_nodes_involved.end() ) {
        _remote_nodes_involved[node_id] = new RemoteNodeInfo;
        _remote_nodes_involved[node_id]->state = RUNNING;
    }
    SundialRequest &request = _remote_nodes_involved[node_id]->request;
    SundialResponse &response = _remote_nodes_involved[node_id]->response;
    request.Clear();
    response.Clear();
    request.set_txn_id( get_txn_id() );
    request.set_request_type( SundialRequest::READ_REQ );
    SundialRequest::ReadRequest * read_request = request.add_read_requests();
    read_request->set_key(key);
    read_request->set_index_id(index_id);
    read_request->set_access_type(access_type);
    grpc_sync_client->contactRemote(node_id,request,&response);
    //handle the response
    assert(response.response_type() == SundialResponse::RESP_OK
           || response.response_type() ==  SundialResponse::RESP_ABORT);
    if (response.response_type() == SundialResponse::RESP_OK) {
        ((LockManager *)_cc_manager)->process_remote_read_response(node_id, access_type, response);
        return RCOK;
    } else {
        _remote_nodes_involved[node_id]->state = ABORTED;
        _is_remote_abort = true;
        return ABORT; 
    }
}

RC
TxnManager::process_2pc_phase1(CompletionQueue* cq)
{
    // Start Two-Phase Commit
    //printf("tnx 2pc p1\n");
    _txn_state = PREPARING;
#if LOG_ENABLE
    char * log_record = NULL;
    uint32_t log_record_size = _cc_manager->get_log_record(log_record);
    //printf("log_record_size is %d\n",log_record_size);
    if (log_record_size > 0) {
        //printf("log record size>0\n");
        assert(log_record);
        log_semaphore->incr();
        //log_manager->log(this, log_record_size, log_record);
        delete [] log_record;
    }
  #if CONTROLLED_LOCK_VIOLATION
    _cc_manager->process_precommit_phase_coord();
  #endif
    _precommit_finish_time = get_sys_clock();
  #if ENABLE_ADMISSION_CONTROL
    //uint64_t wakeup_thread_id = glob_manager->next_wakeup_thread() % g_num_worker_threads;
    //glob_manager->get_worker_thread( wakeup_thread_id )->incr_quota();
    glob_manager->wakeup_next_thread();
  #endif
#endif
    for (auto it = _remote_nodes_involved.begin(); it != _remote_nodes_involved.end(); it ++) {
        assert(it->second->state == RUNNING);
        SundialRequest &request = it->second->request;
        SundialResponse* response = &it->second->response;
        request.Clear();
        response->Clear();
        request.set_txn_id( get_txn_id() );
        request.set_request_type( SundialRequest::PREPARE_REQ );

        ((LockManager *)_cc_manager)->build_prepare_req( it->first, request );

#if ASYNC_RPC
        //rpc_semaphore->incr();
        //rpc_semaphore->print();
        //changed to the sundial grpc async server
        //rpc_client->sendRequestAsync(this, it->first,y request, response);
        grpc::Status status=grpc_async_client->contactRemote(cq,this,it->first,request,&response);
#else
        //rpc_client->sendRequest(it->first, request, response);
        // TODO. for now, assume prepare always succeeds
        assert (response.response_type() == SundialResponse::PREPARED_OK
                || response.response_type() == SundialResponse::PREPARED_OK_RO);
        if (response.response_type() == SundialResponse::PREPARED_OK)
            _remote_nodes_involved[it->first]->state = COMMITTING;
        else
            // the remote sub-txn is readonly and has released locks.
            // For CLV, this means the remote sub-txn does not depend on any
            // weak locks.
            _remote_nodes_involved[it->first]->state = COMMITTED;
#endif
    }
   // printf("finish sending out all async request\n");
    //log_semaphore->wait();
    //printf("reach after log wait\n");
#if ASYNC_RPC
    //printf("reach before rpc wait\n");
    //rpc_semaphore->wait();
     //printf("reach after rpc wait\n");
    for (auto it = _remote_nodes_involved.begin(); it != _remote_nodes_involved.end(); it ++) {
        //modification here, extra statements
        assert(it->second->state == RUNNING);
        SundialResponse &response = it->second->response;
        //printf("try to get response\n");
        grpc_async_client->contactRemoteDone(cq,this,it->first,&response,1);
        //printf("gets response\n");
        assert (response.response_type() == SundialResponse::PREPARED_OK
                || response.response_type() == SundialResponse::PREPARED_OK_RO);
        if (! (response.response_type() == SundialResponse::PREPARED_OK
                || response.response_type() == SundialResponse::PREPARED_OK_RO) )
            cout << response.response_type() << endl;
        if (response.response_type() == SundialResponse::PREPARED_OK)
            it->second->state = COMMITTING;
        else
            it->second->state = COMMITTED;
    }
#endif
    return COMMIT;
}

RC
TxnManager::process_2pc_phase2(RC rc, CompletionQueue* cq)
{
    assert(rc == COMMIT || rc == ABORT);
    _txn_state = (rc == COMMIT)? COMMITTING : ABORTING;
    // TODO. for CLV this logging is optional. Here we use a conservative
    // implementation as logging is not on the critical path of locking anyway.
  #if LOG_ENABLE
    std::string record = std::to_string(_txn_id);
    char * log_record = (char *)record.c_str();
    uint32_t log_record_size = record.length();
    log_semaphore->incr();
    //log_manager->log(this, log_record_size, log_record);
    // OPTIMIZATION: perform local logging and commit request in parallel
    // log_semaphore->wait();
    int count=0;
  #endif
    for (auto it = _remote_nodes_involved.begin(); it != _remote_nodes_involved.end(); it ++) {
        // No need to run this phase if the remote sub-txn has already committed
        // or aborted.
        if (it->second->state == ABORTED || it->second->state == COMMITTED) continue;
        count++;
        SundialRequest &request = it->second->request;
        SundialResponse* response = &it->second->response;
        request.Clear();
        response->Clear();
        request.set_txn_id( get_txn_id() );
        SundialRequest::RequestType type = (rc == COMMIT)?
            SundialRequest::COMMIT_REQ : SundialRequest::ABORT_REQ;
        request.set_request_type( type );
#if ASYNC_RPC
        //rpc_semaphore->incr();
        //rpc_client->sendRequestAsync(this, it->first, request, response);
        grpc_async_client->contactRemote(cq,this,it->first,request,&response);
#else
       // rpc_client->sendRequest(it->first, request, response);
        assert (response.response_type() == SundialResponse::ACK);
        _remote_nodes_involved[it->first]->state = (rc == COMMIT)? COMMITTED : ABORTED;
#endif
    }
    // OPTIMIZATION: release locks as early as possible.
    // No need to wait for this log since it is optional (shared log optimization)
    dependency_semaphore->wait();
    _cc_manager->cleanup(rc);
    //log_semaphore->wait();
#if ASYNC_RPC
    //rpc_semaphore->wait();
    grpc_async_client->contactRemoteDone(cq,this,1,NULL,count);
    for (auto it = _remote_nodes_involved.begin(); it != _remote_nodes_involved.end(); it ++) {
        if (it->second->state == ABORTED || it->second->state == COMMITTED) continue;
        __attribute__((unused)) SundialResponse &response = it->second->response;
        assert (response.response_type() == SundialResponse::ACK);
        it->second->state = 
(rc == COMMIT)? COMMITTED : ABORTED;
    }
#endif
    _txn_state = (rc == COMMIT)? COMMITTED : ABORTED;
    return rc;
}

// RPC Server
// ==========
RC
TxnManager::process_remote_request(const SundialRequest* request, SundialResponse* response)
{
    RC rc = RCOK;
    uint32_t num_tuples;
  #if LOG_ENABLE
    std::string record;
    char * log_record = NULL;
    uint32_t log_record_size = 0;
  #endif
    switch(request->request_type()) {
        case SundialRequest::READ_REQ :
            num_tuples = request->read_requests_size();
            for (int i = 0; i < num_tuples; i++) {
                uint64_t key = request->read_requests(i).key();
                uint64_t index_id = request->read_requests(i).index_id();
                access_t access_type = (access_t)request->read_requests(i).access_type();

                INDEX * index = GET_WORKLOAD->get_index(index_id);
                set<row_t *> * rows = NULL;
                // TODO. all the matching rows should be returned.
                rc = get_cc_manager()->index_read(index, key, rows, 1);
                assert(rc == RCOK || rc == ABORT);
                if (rc == ABORT) break;
                if (!rows) {
                    printf("[txn=%ld] key=%ld, index_id=%ld, access_type=%d\n",
                           get_txn_id(), key, index_id, access_type);
                }
                assert(rows);
                row_t * row = *rows->begin();
                rc = get_cc_manager()->get_row(row, access_type, key);
                if (rc == ABORT) break;
                uint64_t table_id = row->get_table_id();
                SundialResponse::TupleData * tuple = response->add_tuple_data();
                uint64_t tuple_size = row->get_tuple_size();
                tuple->set_key(key);
                tuple->set_table_id( table_id );
                tuple->set_size( tuple_size );
                tuple->set_data( get_cc_manager()->get_data(key, table_id), tuple_size );
            }
            if (rc == ABORT) {
                response->set_response_type( SundialResponse::RESP_ABORT );
                _cc_manager->cleanup(ABORT);
            } else
                response->set_response_type( SundialResponse::RESP_OK );
            //printf("finish precess remote read\n");    
            return rc;

        case SundialRequest::PREPARE_REQ :
            //printf("get prepare request\n");
            // copy data to the write set.
            num_tuples = request->tuple_data_size();
            for (int i = 0; i < num_tuples; i++) {
                uint64_t key = request->tuple_data(i).key();
                uint64_t table_id = request->tuple_data(i).table_id();
                char * data = get_cc_manager()->get_data(key, table_id);
                memcpy(data, request->tuple_data(i).data().c_str(), request->tuple_data(i).size());
            }
           // printf("finish copying data\n");
  #if LOG_ENABLE
            log_record_size = _cc_manager->get_log_record(log_record);
            if (log_record_size > 0) {
              //  printf("log in remote record size>0\n");
                assert(log_record);
                log_semaphore->incr();
                //log_manager->log(this, log_record_size, log_record);
                delete [] log_record;
            }

    #if CONTROLLED_LOCK_VIOLATION
            _cc_manager->process_precommit_phase_coord();
    #endif
            //printf("reach before wait\n");
            //log_semaphore->wait();
            //printf("reach after wait\n");
  #endif
            response->set_response_type( SundialResponse::PREPARED_OK );
            //printf("gets prepared phase ok\n");
            return rc;
        case SundialRequest::COMMIT_REQ :
        case SundialRequest::ABORT_REQ :
            //printf("get commit/abort request\n");
  #if LOG_ENABLE
            record = std::to_string(_txn_id);
            log_record = (char *)record.c_str();
            log_record_size = record.length();
            log_semaphore->incr();
            //log_manager->log(this, log_record_size, log_record);
  #endif
            dependency_semaphore->wait();
            rc = (request->request_type() == SundialRequest::COMMIT_REQ)? COMMIT : ABORT;
            _txn_state = (rc == COMMIT)? COMMITTED : ABORTED;
            _cc_manager->cleanup(rc);
            // OPTIMIZATION: release locks as early as possible.
            // No need to wait for this log since it is optional (shared log
            // optimization)
            //log_semaphore->wait();
            response->set_response_type( SundialResponse::ACK );
            return rc;
        default:
            assert(false);
            exit(0);
    }
}
