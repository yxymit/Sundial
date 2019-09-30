#include "transport.h"
#include "message.h"
#include "global.h"
#include "helper.h"
#include "manager.h"
#include <fcntl.h>
#include <cstring>
#include <errno.h>
#include <netinet/tcp.h>
//////////////////////////////////////////
// Each node listens to port correpsonding to each remote node.
// Each node pushes to the corresponing node's corresponding port
// For 4 nodes, for exmaple,
// node 0 listens to START_PORT + 1 for message from node 1
//                   START_PORT + 2 for message from node 2
//                   START_PORT + 3 for message from node 3
//////////////////////////////////////////

#define PRINT_DEBUG_INFO false
//#define PRINT_DEBUG_INFO true

// The socket code is borrowed from
// http://easy-tutorials.net/c/linux-c-socket-programming/
// The external program execution code is borrowed from http://stackoverflow.com/questions/478898/how-to-execute-a-command-and-get-output-of-command-within-c-using-posix
Transport::Transport(uint32_t transport_id)
{
    _transport_id = transport_id;

    read_urls();
    if (g_num_nodes == 1)
      return;
    _local_info = new addrinfo [g_num_nodes];
    _remote_info = new addrinfo [g_num_nodes];
    _local_socks = new int [g_num_nodes];
    _remote_socks = new int [g_num_nodes];

    _rr_node_id = 0;
    _recv_buffer = new char * [g_num_nodes];
    _recv_buffer_lower = new uint32_t [g_num_nodes];
    _recv_buffer_upper = new uint32_t [g_num_nodes];
    _send_buffer = new char * [g_num_nodes];
    //_send_buffer_size = new uint32_t [g_num_nodes];
    _send_buffer_lower = new uint32_t [g_num_nodes];
    _send_buffer_upper = new uint32_t [g_num_nodes];
    for (uint32_t i = 0; i < g_num_nodes; i ++) {
        _recv_buffer[i] = new char [RECV_BUFFER_SIZE];
        //_recv_buffer_size[i] = 0;
        _recv_buffer_lower[i] = 0;
        _recv_buffer_upper[i] = 0;

        _send_buffer[i] = new char [SEND_BUFFER_SIZE];
        _send_buffer_lower[i] = 0;
        _send_buffer_upper[i] = 0;
    }

    char hostname[1024];
    gethostname(hostname, 1023);
    /*if(std::strlen(hostname) > 9) {  // then this is an AWS host name
        char buffer[128];
        int id_b = 0;
        std::shared_ptr<FILE> pipe(popen("curl http://169.254.169.254/latest/meta-data/local-ipv4", "r"), pclose);
        if (!pipe) throw std::runtime_error("popen() failed!");
        while (!feof(pipe.get())) {
            if (fgets(buffer, 128, pipe.get()) != NULL) {
                std::memcpy(hostname + (id_b << 7), buffer, 128);
                id_b += 1;
            }
        }
        hostname[id_b << 7] = 0;  // in case they don't have a \0
    }*/
    printf("[!] My Hostname is %s\n", hostname);
    uint32_t global_node_id = g_num_nodes;
    for (uint32_t i = 0; i < g_num_nodes; i ++)  {
        if (_urls[i] == string(hostname))
            global_node_id = i;
    }
    M_ASSERT(global_node_id != g_num_nodes, "the node %s is not in ifconfig.txt", hostname);
    if (_transport_id == 0) {
        g_node_id = global_node_id;
        printf("Hostname: %s. Node ID=%d. \n", hostname, global_node_id);
    }

    // Create local socket for each remote server
    // For thread i on a node, it listens to node j on port = START_PORT + i * g_num_nodes + j
    memset(_local_info, 0, sizeof(addrinfo) * g_num_nodes);
    for (uint32_t i = 0; i < g_num_nodes; i ++) {
        if (i == global_node_id)
            continue;
        // get addr information
        _local_info[i].ai_family = AF_UNSPEC;     // IP version not specified. Can be both.
        _local_info[i].ai_socktype = SOCK_STREAM; // Use SOCK_STREAM for TCP or SOCK_DGRAM for UDP.
        _local_info[i].ai_flags = AI_PASSIVE;
          struct addrinfo *host_info_list; // Pointer to the to the linked list of host_info's.
          int status = getaddrinfo(NULL, std::to_string(get_port_num(i)).c_str(), &_local_info[i], &host_info_list);
          assert (status == 0) ;
        // create socket
        int socketfd = socket(host_info_list->ai_family, host_info_list->ai_socktype,
                                  host_info_list->ai_protocol);

        uint32_t recv_buffer_size = 1048576 / 2;
        setsockopt(socketfd, SOL_SOCKET, SO_RCVBUF, &recv_buffer_size, sizeof(uint32_t));

        int flag = 1;
        int res = setsockopt(socketfd, IPPROTO_TCP, TCP_NODELAY, &flag, sizeof(int));
        assert(res != -1);
        //setsockopt(socketfd, SOL_SOCKET, TCP_QUICKACK, &flag, sizeof(int));

        _local_socks[i] = socketfd;
        assert (socketfd != -1);
        // bind socket
        // we make use of the setsockopt() function to make sure the port is not in use.
        int yes = 1;
        status = setsockopt(socketfd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(int));
        status = bind(socketfd, host_info_list->ai_addr, host_info_list->ai_addrlen);
        // set the socket to non-blocking
        assert (status != -1);
        status = listen(socketfd, 5);
        assert(status != -1);
        fcntl(socketfd, F_SETFL, O_NONBLOCK);
    }
    cout << "Local Socket initialized"  << endl;

    // Establish connection to remote servers.
    memset(_remote_info, 0, sizeof(addrinfo) * g_num_nodes);
    for (uint32_t i = 0; i < g_num_nodes; i++) {
        if (i == global_node_id)
            continue;
        // get addr information
        _remote_info[i].ai_family = AF_UNSPEC;     // IP version not specified. Can be both.
        _remote_info[i].ai_socktype = SOCK_STREAM; // Use SOCK_STREAM for TCP or SOCK_DGRAM for UDP.
          struct addrinfo *host_info_list; // Pointer to the to the linked list of host_info's.
        string server_name = _urls[i];
          int status;
        do {
            status = getaddrinfo(server_name.c_str(), std::to_string(get_port_num(global_node_id)).c_str(), &_remote_info[i], &host_info_list);
        } while (status != 0);
        // create socket
        int socketfd;
        do {
            socketfd = socket(host_info_list->ai_family, host_info_list->ai_socktype,
                                  host_info_list->ai_protocol);
        } while (socketfd == -1);
        _remote_socks[i] = socketfd;


        do {
            status = connect(socketfd, host_info_list->ai_addr, host_info_list->ai_addrlen);
            PAUSE;
        } while (status == -1);

        uint32_t send_buffer_size = 1048576 / 2;
        setsockopt(socketfd, SOL_SOCKET, SO_SNDBUF, &send_buffer_size, sizeof(uint32_t));
        int flag = 1;
        //setsockopt(socketfd, SOL_SOCKET, TCP_NODELAY, &flag, sizeof(int));
        int res = setsockopt(socketfd, IPPROTO_TCP, TCP_NODELAY, &flag, sizeof(int));
        assert(res != -1);

        setsockopt(socketfd, SOL_SOCKET, TCP_QUICKACK, &flag, sizeof(int));
        // TODO. right now, sending is blocking. change this later
        //fcntl(socketfd, F_SETFL, O_NONBLOCK);
    }

    // Accept connection from remote servers.
    for (uint32_t i = 0; i < g_num_nodes; i++) {
        if (i == global_node_id)
            continue;
        int new_sd;
        struct sockaddr_storage their_addr;
        socklen_t addr_size = sizeof(their_addr);
        int socketfd = _local_socks[i];
        do {
            new_sd = accept(socketfd, (struct sockaddr *)&their_addr, &addr_size);
        } while (new_sd == -1);
        _local_socks[i] = new_sd;
        fcntl(new_sd, F_SETFL, O_NONBLOCK);
    }

    cout << "Socket initialized" << endl;
    _tot_bytes_sent = 0;
}

uint32_t
Transport::sendMsg(Message * msg)
{
#if !ENABLE_MSG_BUFFER
    uint32_t dest = msg->get_dest_id();
    M_ASSERT(dest < g_num_nodes, "dest=%d", dest);
    uint32_t packet_size = msg->get_packet_len();
    char data[packet_size];
    msg->to_packet(data);
    uint32_t bytes_sent = 0;
    while (bytes_sent < packet_size) {
        if (msg->get_type() == Message::DUMMY) {
            int32_t size = send(_remote_socks[ dest ], data + bytes_sent, packet_size - bytes_sent, MSG_DONTWAIT);
            if (size <= 0 && bytes_sent == 0)
                break;
            else if (size > 0)
                bytes_sent += size;
        } else {
            int32_t size = send(_remote_socks[ dest ], data + bytes_sent, packet_size - bytes_sent, 0);
            if (size > 0)
                bytes_sent += size;
        }
    }
#if PRINT_DEBUG_INFO
    printf("\033[1;31m[TxnID=%5ld] Send %d->%d %16s [%4d bytes] fd=%d \033[0m\n",
           msg->get_txn_id(), GLOBAL_NODE_ID, dest, msg->get_name().c_str(), bytes_sent,
           _remote_socks[dest]);
#endif

    return bytes_sent;
#else
    uint32_t dest = msg->get_dest_id();
    assert(dest < g_num_nodes);

    // Right now use a very simple batching model:
    //     Flush the buffer iff it is full, or it has not been flushed for 100 us.

    // 1. if the upper is full, force sending until the buffer_size (upper - lower)
    //    is small. Then shift the data in the buffer.
    // 2. serialize msg to _send_buffer.
    uint32_t len = msg->get_packet_len();
    int32_t total_bytes = 0;
    int32_t bytes_sent = 0;
    // the upper is close to the end of buffer.
    if (SEND_BUFFER_SIZE - _send_buffer_upper[dest] <= MAX_MESSAGE_SIZE) {
        // the drain the buffer
        while (_send_buffer_upper[dest] - _send_buffer_lower[dest] >= SEND_BUFFER_SIZE / 4) {
            bytes_sent = send(_remote_socks[ dest ],
                              _send_buffer[dest] + _send_buffer_lower[dest],
                              _send_buffer_upper[dest] - _send_buffer_lower[dest], 0);
            if (bytes_sent > 0) {
                _send_buffer_lower[dest] += bytes_sent;
                total_bytes += bytes_sent;
            }
        }
        // shift the buffer.
        if (_send_buffer_upper[dest] > _send_buffer_lower[dest])
            memmove(_send_buffer[dest],
                    _send_buffer[dest] + _send_buffer_lower[dest],
                    _send_buffer_upper[dest] - _send_buffer_lower[dest]);
        _send_buffer_upper[dest] -= _send_buffer_lower[dest];
        _send_buffer_lower[dest] = 0;
    }
    // serialize msg to the send buffer
    assert(SEND_BUFFER_SIZE - _send_buffer_upper[dest] >= MAX_MESSAGE_SIZE);
    msg->to_packet(_send_buffer[dest] + _send_buffer_upper[dest]);
    _send_buffer_upper[dest] += len;

#if PRINT_DEBUG_INFO
    printf("\033[1;31m[TransID=%d] send %d->%d %s txnID=%ld, [%d bytes] fd=%d \033[0m\n",
           _transport_id,
           GLOBAL_NODE_ID, dest, msg->get_name().c_str(), msg->get_txn_id(), len,
           _remote_socks[dest]);
#endif
    return total_bytes; //_send_buffer_upper[dest] - _send_buffer_lower;
#endif
}

void
Transport::sendBufferedMsg()
{
    //bool done = true;
    for (uint32_t dest = 0; dest < g_num_nodes; dest++) {
        while (_send_buffer_lower[dest] != _send_buffer_upper[dest]) {
            int32_t bytes_sent = send(_remote_socks[ dest ],
                                       _send_buffer[dest] + _send_buffer_lower[dest],
                                       _send_buffer_upper[dest] - _send_buffer_lower[dest],
                                       0);
            if (bytes_sent > 0)
                _send_buffer_lower[dest] += bytes_sent;
        }
        _send_buffer_lower[dest] = 0;
        _send_buffer_upper[dest] = 0;
    }
}

Message *
Transport::recvMsg()
{
    ssize_t bytes;
    //char data[MAX_MESSAGE_SIZE];
    uint32_t global_node_id = g_node_id;
    for (uint32_t i = 0; i < g_num_nodes; i++) {
        uint32_t node_id = (i + _rr_node_id) % g_num_nodes;
        if (node_id == global_node_id)
            continue;

        if (_recv_buffer_upper[node_id] - _recv_buffer_lower[node_id] <= MAX_MESSAGE_SIZE) {
            // not enough bytes in the recv buffer, should recv from network.
            if (RECV_BUFFER_SIZE - _recv_buffer_upper[node_id] < RECV_BUFFER_SIZE / 4) {
                // if the recv_buffer is close to full, shift it.
                memmove(_recv_buffer[node_id],
                         _recv_buffer[node_id] + _recv_buffer_lower[node_id],
                        _recv_buffer_upper[node_id] - _recv_buffer_lower[node_id]);
                _recv_buffer_upper[node_id] -= _recv_buffer_lower[node_id];
                _recv_buffer_lower[node_id] = 0;
            }
            uint32_t max_size = RECV_BUFFER_SIZE - _recv_buffer_upper[node_id];
//uint64_t tt = get_sys_clock();
            bytes = recv(_local_socks[node_id],
                             _recv_buffer[node_id] + _recv_buffer_upper[node_id],
                          max_size, MSG_DONTWAIT);
//if (stats)
//INC_FLOAT_STATS(time_debug1, get_sys_clock() - tt);
            if (bytes > 0)
                _recv_buffer_upper[node_id] += bytes;
        }

        if (_recv_buffer_upper[node_id] - _recv_buffer_lower[node_id] >= sizeof(Message)) {
            Message * msg = (Message *) (_recv_buffer[node_id] + _recv_buffer_lower[node_id]);
            assert(msg->get_packet_len() < MAX_MESSAGE_SIZE);
            if (_recv_buffer_upper[node_id] - _recv_buffer_lower[node_id] >= msg->get_packet_len()) {
                // Find a valid input message
                msg = (Message *) MALLOC(sizeof(Message));
                new(msg) Message(_recv_buffer[node_id] + _recv_buffer_lower[node_id]);
                _recv_buffer_lower[node_id] += msg->get_packet_len();
                if (_recv_buffer_upper[node_id] == _recv_buffer_lower[node_id]) {
                    // a small optimization
                    _recv_buffer_lower[node_id] = 0;
                    _recv_buffer_upper[node_id] = 0;
                }
//INC_STATS(0, debug3, get_sys_clock() - t1);
                //_recv_buffer_size[node_id] -= msg->get_packet_len();
                _rr_node_id = (1 + _rr_node_id) % g_num_nodes;
#if PRINT_DEBUG_INFO
                printf("\033[1;32m[TxnID=%5ld] recv %d<-%d %16s [%4d bytes] fd=%d \033[0m\n",
                       msg->get_txn_id(), global_node_id, msg->get_src_node_id(),
                       msg->get_name().c_str(), msg->get_packet_len(),
                       _local_socks[node_id]);
#endif
                return msg;
            }
        }
    }
    _rr_node_id = (1 + _rr_node_id) % g_num_nodes;
    return NULL;
}

void
Transport::test_connect()
{
    uint32_t global_node_id = g_node_id;
    // send msg to all nodes.
    for (uint32_t i = 0; i < g_num_nodes; i++) {
        if (i == global_node_id)
            continue;
        //Message * msg = new Message(Message::TERMINATE, i, 0, 0, NULL);
        Message * msg;
        NEW(msg, Message, Message::TERMINATE, i, 0, 0, NULL);
        uint32_t bytes = sendMsg(msg);
        if (bytes != msg->get_packet_len())
            sendBufferedMsg();
        DELETE(Message, msg); //delete msg;
    }
    // receive msg from all nodes
    for (uint32_t i = 0; i < g_num_nodes - 1; i++) {
        Message * msg;
        do {
            msg = recvMsg();
        } while (msg == NULL);
        DELETE(Message, msg); //delete msg;
        //delete msg;
    }
    cout << "Connection Test PASS!" << endl;
}

void
Transport::read_urls()
{
    // get server names
    string line;
    ifstream file (ifconfig_file);
    assert(file.is_open());
    uint32_t num_server_nodes = 0;
    uint32_t num_monitor_nodes = 0;
    uint32_t node_type = (uint32_t)-1;
    while (getline (file, line)) {
        if (line[0] == '#')
            continue;
        if (line[0] == '=') {
            switch (line[1]) {
                case 'c' : node_type = 0; break;
                case 's' : node_type = 1; break;
                case 'm' : node_type = 2; break;
            }
        }
        else {
            switch (node_type) {
                case 0: { break; }
                case 1: { num_server_nodes ++; _urls.push_back(line); break; }
                case 2: { num_monitor_nodes ++; _urls.push_back(line); break; }
            }
        }
    }
    assert(num_monitor_nodes <= 1);
    g_num_nodes = num_server_nodes + num_monitor_nodes;
    g_num_server_nodes = num_server_nodes;
    file.close();
}

uint32_t
Transport::get_port_num(uint32_t node_id)
{
    //return START_PORT + thd_id * g_num_nodes + node_id;
    return START_PORT + node_id * g_num_input_threads + _transport_id;
}
