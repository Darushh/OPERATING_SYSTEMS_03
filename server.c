#include "segel.h"
#include "request.h"
#include "log.h"
#include "queue.h"

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

typedef struct udp_request {
    struct sockaddr_in client_addr;
    socklen_t client_len;
    struct udp_request *next;
} udp_request_t;

/* Per-worker persistent state. 
   Each worker has its own ID and statistics object,
   while all workers share the same request queue and server log.
*/
typedef struct {
    int thread_id;
    request_queue_t *queue;
    server_log log;

    struct Threads_stats stats;

    int udpfd;

    // UDP Request intended specifically for this worker.
    // The worker should handle these before taking another TCP job.
    udp_request_t *udp_head;
    udp_request_t *udp_tail;
} worker_context_t;

// Add a UDP ping to the queue of its target worker.
// The TCP queue mutex also protects all per-worker UDP queues.
void enqueue_udp_request(
    worker_context_t *worker,
    struct sockaddr_in *client_addr,
    socklen_t client_len
)
{
    udp_request_t *request = malloc(sizeof(udp_request_t));

    if (request == NULL) {
        unix_error("malloc error");
    }

    request->client_addr = *client_addr;
    request->client_len = client_len;
    request->next = NULL;

    pthread_mutex_lock(&worker->queue->mutex);

    if (worker->udp_tail == NULL) {
        worker->udp_head = request;
        worker->udp_tail = request;
    } else {
        worker->udp_tail->next = request;
        worker->udp_tail = request;
    }

    // A worker may currently sleep because no TCP job exists.
    // Wake workers so the target worker can notice its UDP ping.
    pthread_cond_broadcast(&worker->queue->not_empty);

    pthread_mutex_unlock(&worker->queue->mutex);
}

// Remove one pending UDP ping.
// Precondition: worker->queue->mutex is already locked.
udp_request_t *dequeue_udp_request_locked(worker_context_t *worker)
{
    udp_request_t *request = worker->udp_head;

    if (request != NULL) {
        worker->udp_head = request->next;

        if (worker->udp_head == NULL) {
            worker->udp_tail = NULL;
        }
    }
    return request;
}

// Send this worker's current statistics back to the UDP client.
// UDP pings do not count as HTTP jobs and do not update any counters.
void handle_udp_request(worker_context_t *worker, udp_request_t *request)
{
    char response[MAXLINE] = "";

    int response_len = append_thread_log(response, &worker->stats);

    UDP_Write(
        worker->udpfd,
        &request->client_addr,
        response,
        response_len
    );
}

// Worker thread routine: repeatedly takes the oldest request and handles it. 
void *worker_main(void *arg)
{
    worker_context_t *context = (worker_context_t *)arg;

    while (1) {
        udp_request_t *udp_request = NULL;
        request_job_t tcp_job;
        int handling_udp = 0;

        pthread_mutex_lock(&context->queue->mutex);

        // Sleep only when this worker has no UDP ping and there are no TCP jobs at all.
        while (context->udp_head == NULL && context->queue->count == 0) {
            pthread_cond_wait(
                &context->queue->not_empty,
                &context->queue->mutex
            );
        }

        // UDP has priority for this worker.
        // A UDP ping is never inserted into the TCP FIFO queue.
        if (context->udp_head != NULL) {
            udp_request = dequeue_udp_request_locked(context);
            handling_udp = 1;
        } else {
            tcp_job = queue_dequeue_locked(context->queue);
        }

        pthread_mutex_unlock(&context->queue->mutex);

        if (handling_udp) 
        {
            handle_udp_request(context, udp_request);
            free(udp_request);
            continue;
        }

        gettimeofday(&tcp_job.time_stats.task_dispatch, NULL);

        requestHandle(
            tcp_job.connfd,
            tcp_job.time_stats,
            &context->stats,
            context->log
        );

        Close(tcp_job.connfd);
    }

    return NULL;
}

int main(int argc, char *argv[])
{
    // Create the global server log
    server_log log;

    int listenfd, connfd, udpfd;
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
    log = create_log(debug_sleep_time);
    // Dara's part 
    (void)debug_sleep_time;

    request_queue_t request_queue;
    // Shared bounded FIFO queue used by the master and all workers.
    queue_init(&request_queue, queue_size);

    pthread_t *worker_threads = malloc(sizeof(pthread_t) * num_threads);

    worker_context_t *worker_contexts = malloc(sizeof(worker_context_t) * num_threads);

    if (worker_threads == NULL || worker_contexts == NULL) {
        unix_error("malloc error");
    }

    listenfd = Open_listenfd(tcp_port);
    udpfd = UDP_Open(udp_port);

    // Create a fixed-size worker pool once at server startup.
    for (int i = 0; i < num_threads; i++) 
    {
        worker_contexts[i].thread_id = i + 1;
        worker_contexts[i].queue = &request_queue;
        worker_contexts[i].log = log;
        worker_contexts[i].udpfd = udpfd;


        worker_contexts[i].stats.id = i + 1;
        worker_contexts[i].stats.stat_req = 0;
        worker_contexts[i].stats.dynm_req = 0;
        worker_contexts[i].stats.post_req = 0;
        worker_contexts[i].stats.total_req = 0;

        worker_contexts[i].udp_head = NULL;
        worker_contexts[i].udp_tail = NULL;

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

    while (1) 
    {
        fd_set readfds;

        FD_ZERO(&readfds);
        FD_SET(listenfd, &readfds);
        FD_SET(udpfd, &readfds);

        int maxfd = (listenfd > udpfd) ? listenfd : udpfd;

    // Sleep until either:
    // 1) a new TCP connection arrives
    // 2) a UDP statistics ping arrives
    Select(maxfd + 1, &readfds, NULL, NULL, NULL);

    // Handle UDP first when both sockets are ready - because of the UDP priority.
    if (FD_ISSET(udpfd, &readfds)) {
        char buffer[MAXLINE];
        struct sockaddr_in udp_clientaddr;

        int bytes_read = UDP_Read(
            udpfd,
            &udp_clientaddr,
            buffer,
            MAXLINE - 1
        );

        if (bytes_read > 0) {
            buffer[bytes_read] = '\0';

            char *endptr;
            long requested_id = strtol(buffer, &endptr, 10);

            // Valid UDP message format for now: ASCII thread ID, for example "1" or "3".
            if (endptr != buffer && requested_id >= 1 && requested_id <= num_threads) {
                enqueue_udp_request(
                    &worker_contexts[requested_id - 1],
                    &udp_clientaddr,
                    sizeof(udp_clientaddr)
                );
            }
        }
    }

    if (FD_ISSET(listenfd, &readfds)) {
        struct sockaddr_in clientaddr;
        socklen_t clientlen = sizeof(clientaddr);

        connfd = Accept(
            listenfd,
            (SA *)&clientaddr,
            &clientlen
        );

        request_job_t job;
        job.connfd = connfd;

        gettimeofday(&job.time_stats.task_arrival, NULL);

        // Dare's Part - now it is temporary log timestamps.
        // please replace these inside the real logger
        job.time_stats.log_enter = job.time_stats.task_arrival;
        job.time_stats.log_exit = job.time_stats.task_arrival;

        queue_enqueue(&request_queue, job);
    }
}


    Close(listenfd);
    Close(udpfd);
    queue_destroy(&request_queue);
    destroy_log(log);

    free(worker_threads);
    free(worker_contexts);

    return 0;
}
