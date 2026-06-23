#include "segel.h"
#include "request.h"
#include "log.h"

//
// server.c: A very, very simple web server
//
// To run:
//  ./server <portnum (above 2000)>
//
// Repeatedly handles HTTP requests sent to this port number.
// Most of the work is done within routines written in request.c
//

// Parses command-line arguments
void getargs(int *tcp_port, int *udp_port, int *num_threads, int *queue_size, 
    double *debug_sleep_time, int argc, char *argv[])
{
    if (argc != 6)
    {
        fprintf(
            stderr,
            "Usage: %s <tcp_port> <udp_port> <threads> <queue_size> <debug_sleep_time>\n",
            argv[0]
        );
        exit(1);
    }
    *tcp_port = atoi(argv[1]);
    *udp_port = atoi(argv[2]);
    *num_threads = atoi(argv[3]);
    *queue_size = atoi(argv[4]);
    *debug_sleep_time = atof(argv[5]);

    if (*tcp_port <= 1024 || *tcp_port > 65535)
    {
        fprintf(stderr, "TCP port must be between 1025 and 65535\n");
        exit(1);
    }

    if (*udp_port <= 1024 || *udp_port > 65535)
    {
        fprintf(stderr, "UDP port must be between 1025 and 65535\n");
        exit(1);
    }

    if (*tcp_port == *udp_port)
    {
         fprintf(stderr, "TCP and UDP ports must be different\n");
        exit(1);
    }

    if (*num_threads <= 0)
    {
        fprintf(stderr, "Number of threads must be positive\n");
        exit(1);
    }

    if (*queue_size <= 0) {
        fprintf(stderr, "Queue size must be positive\n");
        exit(1);
    }
}

// TODO: HW3 — Task 1: Initialize the thread pool and request queue.
// This server currently handles all requests in the main thread.

// TODO: HW3 — Task 4: Add the UDP channel (see the UDP_* wrappers in segel.c).

// TODO: HW3 — Extend getargs() to parse the full argument list.

int main(int argc, char *argv[])
{
    // Create the global server log
    server_log log = create_log();

    int listenfd, connfd, clientlen;
    int tcp_port, udp_port, num_threads, queue_size;
    double debug_sleep_time;
    struct sockaddr_in clientaddr;

    getargs(&tcp_port,&udp_port,&num_threads,&queue_size,&debug_sleep_time,argc,argv);

    listenfd = Open_listenfd(tcp_port);

    /*  thread pool, queue, UDP, log debug sleep. */
    (void)udp_port;
    (void)num_threads;
    (void)queue_size;
    (void)debug_sleep_time;

    while (1) {
        clientlen = sizeof(clientaddr);
        connfd = Accept(listenfd, (SA *)&clientaddr, (socklen_t*) &clientlen);

        // TODO: HW3 — Record the request arrival time here.

        // DEMO PURPOSE ONLY:
        // This is a dummy request handler that immediately processes the
        // request in the master thread without concurrency. Replace this with
        // logic that enqueues the connection so a worker thread handles it.

        threads_stats t = malloc(sizeof(struct Threads_stats));
        t->id = 0;             // Thread ID (placeholder)
        t->stat_req = 0;       // Static request count
        t->dynm_req = 0;       // Dynamic request count
        t->post_req = 0;       // POST request count
        t->total_req = 0;      // Total request count

        time_stats dum;

        // gettimeofday(&arrival, NULL);

        // Call the request handler (immediate in master thread — DEMO ONLY)
        requestHandle(connfd, dum, t, log);

        free(t); // Cleanup
        Close(connfd); // Close the connection
    }

    // Clean up the server log before exiting
    destroy_log(log);

    // TODO: HW3 — Add cleanup code for the thread pool and queue.
}
