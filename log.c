#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include "log.h"

struct Server_Log {
    char *buffer;      //buffer saves all the log
    int size;          //logs size
    int capacity;      //current buffer's size
    
    //manegment of shared resources:
    pthread_mutex_t mutex;   
    pthread_cond_t read_cond;
    pthread_cond_t write_cond;
    
    //counters for mangment of readers and writers
    int active_readers;
    int waiting_readers;
    int active_writers;
    int waiting_writers; 
    
    double debug_sleep_time; //sleep time for debugging
};

// Creates a new server log instance (stub)
server_log create_log() {
    Server_Log* log = (Server_Log)malloc(sizeof(struct Server_Log));
    if (!log) return NULL;
    
    
}

// Destroys and frees the log (stub)
void destroy_log(server_log log) {
    // TODO: Free all internal resources used by the log
    free(log);
}

// Returns dummy log content as string (stub)
int get_log(server_log log, char** dst) {
    // TODO: Return the full contents of the log as a dynamically allocated string
    // This function should handle concurrent access
}

// Appends a new entry to the log (no-op stub)
void add_to_log(server_log log, const char* data, int data_len) {
    // TODO: Append the provided data to the log
    // This function should handle concurrent access
}
