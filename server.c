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

    // UDP Request intended specifically for this worker.
    // The worker should handle these before taking another TCP job.
    udp_request_t *udp_head;
    udp_request_t *udp_tail;

    pthread_mutex_t udp_mutex;
} worker_context_t;

// Helper functions for the per-worker UDP queue.
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

    pthread_mutex_lock(&worker->udp_mutex);

    if (worker->udp_tail == NULL) {
        worker->udp_head = request;
        worker->udp_tail = request;
    } else {
        worker->udp_tail->next = request;
        worker->udp_tail = request;
    }

    pthread_mutex_unlock(&worker->udp_mutex);
}

udp_request_t *dequeue_udp_request(worker_context_t *worker)
{
    pthread_mutex_lock(&worker->udp_mutex);

    udp_request_t *request = worker->udp_head;

    if (request != NULL) {
        worker->udp_head = request->next;

        if (worker->udp_head == NULL) {
            worker->udp_tail = NULL;
        }
    }

    pthread_mutex_unlock(&worker->udp_mutex);

    return request;
}

// Worker thread routine: repeatedly takes the oldest request and handles it. 
void *worker_main(void *arg)
{
    worker_context_t *context = (worker_context_t *)arg;

    while (1) {
        // Wait for and remove the next FIFO request from the shared queue. 
        request_job_t job = queue_dequeue(context->queue);

        gettimeofday(&job.time_stats.task_dispatch, NULL);

        //  The queue mutex is already released here. 
        // Therefore, other workers can continue dequeuing while this request is handled.
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

    // Dara's part 
    (void)udp_port;
    (void)debug_sleep_time;

    request_queue_t request_queue;
    // Shared bounded FIFO queue used by the master and all workers.
    queue_init(&request_queue, queue_size);

    pthread_t *worker_threads = malloc(sizeof(pthread_t) * num_threads);

    worker_context_t *worker_contexts = malloc(sizeof(worker_context_t) * num_threads);

    if (worker_threads == NULL || worker_contexts == NULL) {
        unix_error("malloc error");
    }
    // Create a fixed-size worker pool once at server startup.
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

        worker_contexts[i].udp_head = NULL;
        worker_contexts[i].udp_tail = NULL;

        pthread_mutex_init(&worker_contexts[i].udp_mutex, NULL);

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

        //  Master thread accepts incoming TCP connections.
        connfd = Accept(
            listenfd,
            (SA *)&clientaddr,
            (socklen_t *)&clientlen
        );

        request_job_t job;
        job.connfd = connfd;

        // Record the first moment the server sees this request.
        gettimeofday(&job.time_stats.task_arrival, NULL);

        // Dara's part - 
        // Now these are temporary placeholder values and we need to replace them with real timestaps:
        // log_enter - immediately before requesting the log reader/writer lock.
        // log_exit - after the log operation releases its lock.
        job.time_stats.log_enter = job.time_stats.task_arrival;
        job.time_stats.log_exit = job.time_stats.task_arrival;

        // Add the request to the bounded FIFO queue. 
        // If the queue is full, the master blocks inside queue_enqueue.
        queue_enqueue(&request_queue, job);
    }


    Close(listenfd);
    queue_destroy(&request_queue);
    destroy_log(log);

    free(worker_threads);
    free(worker_contexts);

    return 0;
}
