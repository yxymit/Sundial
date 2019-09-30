#pragma once

#include "global.h"
#include <sys/socket.h>
#include <netdb.h>

class Message;

// For now, each worker thread has its own Transport
// The ith thread on one node only talks to ith thread on another node.
class Transport
{
public:
    Transport(uint32_t transport_id);
    // returns the number of bytes sent.
    uint32_t sendMsg(Message * msg);
    // retuens if all buffered messages are sent.
    void sendBufferedMsg();
    Message * recvMsg();

    void terminate();

    void test_connect();
private:
    void read_urls();
    uint32_t get_port_num(uint32_t node_id);

    // with multiple input/output threads, each pair uses a separate Transport.
    uint32_t             _transport_id;

    vector<string> _urls;
    // For outputs, remote nodes information
    struct addrinfo *     _local_info;
    struct addrinfo *     _remote_info;

    int *                 _local_socks;
    int    *                _remote_socks;

    char **             _recv_buffer;
    //uint32_t *         _recv_buffer_size;
    uint32_t *             _recv_buffer_lower;
    uint32_t *             _recv_buffer_upper;

    char **             _send_buffer;
    uint32_t *             _send_buffer_lower;
    uint32_t *             _send_buffer_upper;

    uint64_t             _rr_node_id;
    // Stats
    uint64_t             _tot_bytes_sent;
};
