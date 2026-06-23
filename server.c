#include "segel.h"
#include "request.h"
#include "log.h"
#include "queue.h"

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

typedef struct {
    int thread_id;
    request_queue_t *queue;
    server_log log;

    struct Threads_stats stats;
} worker_context_t;

void *worker_main(void *arg)
{
    worker_context_t *context = (worker_context_t *)arg;

    while (1) {
        request_job_t job = queue_dequeue(context->queue);

        gettimeofday(&job.time_stats.task_dispatch, NULL);

        requestHandle(
            job.connfd,
            job.time_stats,
            &context->stats,
            context->log
        );

        Close(job.connfd);
    }

    return NULL;
}

// TODO: HW3 — Task 4: Add the UDP channel (see the UDP_* wrappers in segel.c).


// TODO: HW3 — Extend getargs() to parse the full argument list.

int main(int argc, char *argv[])
{
    // Create the global server log
    server_log log = create_log();

    int listenfd, connfd, clientlen;
    int tcp_port, udp_port, num_threads, queue_size;
    double debug_sleep_time;

    getargs(
        &tcp_port,
        &udp_port,
        &num_threads,
        &queue_size,
        &debug_sleep_time,
        argc,
        argv
    );

    // These will be used later for UDP support and debug sleep inside the log. //
    (void)udp_port;
    (void)debug_sleep_time;

    request_queue_t request_queue;
    queue_init(&request_queue, queue_size);

    pthread_t *worker_threads = malloc(sizeof(pthread_t) * num_threads);

    worker_context_t *worker_contexts = malloc(sizeof(worker_context_t) * num_threads);

    if (worker_threads == NULL || worker_contexts == NULL) {
        unix_error("malloc error");
    }

    for (int i = 0; i < num_threads; i++) 
    {
        worker_contexts[i].thread_id = i + 1;
        worker_contexts[i].queue = &request_queue;
        worker_contexts[i].log = log;

        worker_contexts[i].stats.id = i + 1;
        worker_contexts[i].stats.stat_req = 0;
        worker_contexts[i].stats.dynm_req = 0;
        worker_contexts[i].stats.post_req = 0;
        worker_contexts[i].stats.total_req = 0;

        int rc = pthread_create(
            &worker_threads[i],
            NULL,
            worker_main,
            &worker_contexts[i]
        );

        if (rc != 0) {
            posix_error(rc, "pthread_create error");
        }
    }

    listenfd = Open_listenfd(tcp_port);

    while (1) {
        struct sockaddr_in clientaddr;
        clientlen = sizeof(clientaddr);

        connfd = Accept(
            listenfd,
            (SA *)&clientaddr,
            (socklen_t *)&clientlen
        );

        request_job_t job;
        job.connfd = connfd;

        gettimeofday(&job.time_stats.task_arrival, NULL);

        job.time_stats.log_enter = job.time_stats.task_arrival;
        job.time_stats.log_exit = job.time_stats.task_arrival;

        queue_enqueue(&request_queue, job);
    }


    Close(listenfd);
    queue_destroy(&request_queue);
    destroy_log(log);

    free(worker_threads);
    free(worker_contexts);

    return 0;
}
