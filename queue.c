#include "queue.h"

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

request_job_t queue_dequeue(request_queue_t *queue)
{
    request_job_t job;

    pthread_mutex_lock(&queue->mutex);

    while (queue->count == 0) {
        pthread_cond_wait(&queue->not_empty, &queue->mutex);
    }

    job = queue->jobs[queue->head];
    queue->head = (queue->head + 1) % queue->capacity;
    queue->count--;

    pthread_cond_signal(&queue->not_full);

    pthread_mutex_unlock(&queue->mutex);

    return job;
}

void queue_destroy(request_queue_t *queue)
{
    free(queue->jobs);

    pthread_mutex_destroy(&queue->mutex);
    pthread_cond_destroy(&queue->not_empty);
    pthread_cond_destroy(&queue->not_full);
}