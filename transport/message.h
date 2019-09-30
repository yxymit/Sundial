#pragma once

#include "global.h"

class Message
{
public:
    enum Type {
        CLIENT_REQ,
        CLIENT_RESP,
        REQ,
        //RENEW,
        RESP_COMMIT,
        RESP_ABORT,
        // the responses for LOCK_WS_REQ are RESP_COMMIT and RESP_ABORT

        // For naive TicToc or naive Silo, we need three phase commit.
        // The following lock phase is only for these two algorithms
        LOCK_REQ,
        LOCK_COMMIT,
        LOCK_ABORT,
        // Two Phase Commit Messages.
        PREPARE_REQ,
        AMEND_REQ,
        PREPARED_COMMIT,
        PREPARED_ABORT,
        COMMITTED,
        // if a remote node is readonly, after successful prepare, it will directly commit.
        // This optimization only works for 2PL and TicToc but not for F1.
        COMMIT_REQ,
        ABORT_REQ,
        ACK,
        // for global synchronization
        TERMINATE,
        DUMMY,        // for amazon
        // for local caching
        //LOCAL_COPY_REQ,
        LOCAL_COPY_RESP,
        LOCAL_COPY_NACK,

        // For TCM
        TCM_TS_SYNC_REQ,
        NUM_MSG_TYPES
    };
    Message(Type type, uint32_t dest, uint64_t txn_id, int size, char * data);
    Message(Message * msg);
    Message(char * packet);
    ~Message();

    uint32_t get_packet_len();
    uint32_t get_dest_id()         { return _dest_node_id; }
    void set_dest_id(uint32_t dest)    { _dest_node_id = dest; }
    uint32_t get_src_node_id()    { return _src_node_id; }

    uint32_t get_data_size()    { return _data_size; }
    void set_data(char * data)  { _data = data; }
    uint32_t get_data(char * &data)    { data = _data; return _data_size; }
    char * get_data()            { return _data; }
    Type get_type()             { return _msg_type; }
    void set_type(Type type)     { _msg_type = type; }
    uint64_t get_txn_id()        { return _txn_id; }


    void to_packet(char * packet);
    static string get_name(Type type);
    static bool is_response(Type type);
    string get_name() { return get_name(get_type()); }
    bool is_response() { return is_response(get_type()); }
private:
    Type         _msg_type;
    uint32_t     _src_node_id;
    uint32_t     _dest_node_id;
    uint64_t     _txn_id;

    uint32_t    _data_size;
    char *         _data;
};
