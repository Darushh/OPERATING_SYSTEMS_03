#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include "log.h"

struct Server_Log {
    char *buffer;               //dynamic text buffer
    int size;                   //curr log length
    int capacity;               //max size for log currently
    
    //managing synchronization:
    pthread_mutex_t mutex;
    pthread_cond_t read_cond;
    pthread_cond_t write_cond;
    
    //counters for readers and writers - preffering writers
    int active_readers;
    int waiting_readers;
    int active_writers;
    int waiting_writers;
    
    double debug_sleep_time;    //sleep time for debugging
};


// Creates a new server log instance (stub)
server_log create_log(double debug_sleep_time) {
    server_log log = (server_log)malloc(sizeof(struct Server_Log));
    if (!log) return NULL;
    log->capacity = 1024; //starting with 1024 bytes size
    log->buffer = (char*)malloc(log->capacity);
    if (log->buffer == NULL) {
        free(log);
        return NULL;
    }
    log->buffer[0] = '\0'; //log is empty at first
    log->size = 0;
    log->debug_sleep_time = debug_sleep_time;
    //initializing counters:
=   log->active_readers = 0;
    log->waiting_readers = 0;
    log->active_writers = 0;
    log->waiting_writers = 0;
    //initializing mutex & cond variables:
    pthread_mutex_init(&log->mutex, NULL);
    pthread_cond_init(&log->read_cond, NULL);
    pthread_cond_init(&log->write_cond, NULL);
    return log;
}

// Destroys and frees the log (stub)
void destroy_log(server_log log) {
    if (!log) return;
    if (log->buffer) free(log->buffer);
    pthread_mutex_destroy(&log->mutex);
    pthread_cond_destroy(&log->read_cond);
    pthread_cond_destroy(&log->write_cond);
    free(log);
}

// Returns dummy log content as string (stub)
int get_log(server_log log, char** dst) {
    // TODO: Return the full contents of the log as a dynamically allocated string
    // This function should handle concurrent access

    const char* dummy = "Log is not implemented.\n";
    int len = strlen(dummy);
    *dst = (char*)malloc(len + 1); // Allocate for caller
    if (*dst != NULL) {
        strcpy(*dst, dummy);
    }
    return len;
}

// Appends a new entry to the log (no-op stub)
void add_to_log(server_log log, const char* data, int data_len) {
    // TODO: Append the provided data to the log
    // This function should handle concurrent access
}
