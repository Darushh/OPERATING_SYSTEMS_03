#include "queue.h"

// Initialize an empty bounded circular FIFO queue. 
void queue_init(request_queue_t *queue, int capacity)
{
    queue->jobs = malloc(sizeof(request_job_t) * capacity);

    if (queue->jobs == NULL) {
        unix_error("malloc error");
    }

    queue->capacity = capacity;
    queue->head = 0;
    queue->tail = 0;
    queue->count = 0;

    pthread_mutex_init(&queue->mutex, NULL);
    pthread_cond_init(&queue->not_empty, NULL);
    pthread_cond_init(&queue->not_full, NULL);
}
// Add a request to the tail of the queue. 
// Blocks the master thread while the queue is full.
void queue_enqueue(request_queue_t *queue, request_job_t job)
{
    pthread_mutex_lock(&queue->mutex);

    while (queue->count == queue->capacity) {
        pthread_cond_wait(&queue->not_full, &queue->mutex);
    }

    queue->jobs[queue->tail] = job;
    queue->tail = (queue->tail + 1) % queue->capacity;
    queue->count++;

    pthread_cond_signal(&queue->not_empty);

    pthread_mutex_unlock(&queue->mutex);
}

request_job_t queue_dequeue_locked(request_queue_t *queue)
{
    request_job_t job = queue->jobs[queue->head];

    queue->head = (queue->head + 1) % queue->capacity;
    queue->count--;

    // The master may now enqueue another TCP request. 
    pthread_cond_signal(&queue->not_full);

    return job;
}

// Remove the oldest request from the head of the queue. 
// Blocks a worker thread while the queue is empty.
request_job_t queue_dequeue(request_queue_t *queue)
{
    pthread_mutex_lock(&queue->mutex);

    while (queue->count == 0) {
        pthread_cond_wait(&queue->not_empty, &queue->mutex);
    }

    request_job_t job = queue_dequeue_locked(queue);

    pthread_mutex_unlock(&queue->mutex);

    return job;
}
/* Release the queue's allocated memory and synchronization primitives. */
void queue_destroy(request_queue_t *queue)
{
    free(queue->jobs);

    pthread_mutex_destroy(&queue->mutex);
    pthread_cond_destroy(&queue->not_empty);
    pthread_cond_destroy(&queue->not_full);
}