#ifndef __QUEUE_H__
#define __QUEUE_H__

#include "segel.h"
#include "request.h"

/*
A single TCP request waiting to be handled by a worker.
The master thread fills task_arrival.
The worker fills task_dispatch when it removes the job from the queue.
*/
typedef struct {
    int connfd;
    time_stats time_stats;
} request_job_t;

/*
Bounded circular FIFO queue shared by 
one producer (the master thread) 
and mulitiple consumers: worker threads
*/
typedef struct {
    request_job_t *jobs;
    int capacity;
    int head;
    int tail;
    int count;

    pthread_mutex_t mutex;
    pthread_cond_t not_empty;
    pthread_cond_t not_full;
} request_queue_t;

void queue_init(request_queue_t *queue, int capacity);

void queue_enqueue(request_queue_t *queue, request_job_t job);

request_job_t queue_dequeue(request_queue_t *queue);

void queue_destroy(request_queue_t *queue);

#endif