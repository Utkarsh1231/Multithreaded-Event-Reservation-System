#include <iostream>
#include <vector>
#include <pthread.h>
#include <semaphore.h>
#include <unistd.h>
#include <cstdlib>
#include <ctime>

using namespace std;

// System configurations
int num_events, capacity, num_threads, max_queries, runtime;

// POSIX Semaphore for system-wide admission control
sem_t max_query_semaphore;

// Per-event structure with individual readers-writers lock
struct Event {
    int available_seats;
    pthread_rwlock_t rw_lock;
};

vector<Event> events;

// To track bookings per thread for cancellations
struct Booking {
    int event_no;
    int seats_booked;
};

void read_query(int event_no, int thread_id) {
    // READER: Shared lock allows multiple concurrent reads
    pthread_rwlock_rdlock(&events[event_no].rw_lock);
    printf("Thread:%d [READ] Event %d has %d available seats.\n", thread_id, event_no, events[event_no].available_seats);
    pthread_rwlock_unlock(&events[event_no].rw_lock);
}

void book_ticket(int event_no, int thread_id, vector<Booking>& thread_bookings, unsigned int& seed) {
    int k = (rand_r(&seed) % 6) + 5; // Book 5 to 10 seats
    
    // WRITER: Exclusive lock prevents race conditions during booking
    pthread_rwlock_wrlock(&events[event_no].rw_lock);
    if (k <= events[event_no].available_seats) {
        events[event_no].available_seats -= k;
        thread_bookings.push_back({event_no, k});
        printf("Thread:%d [BOOK] Successfully booked %d seats for Event %d. Seats left: %d\n", thread_id, k, event_no, events[event_no].available_seats);
    } else {
        printf("Thread:%d [BOOK-FAIL] Requested %d seats, but Event %d only has %d seats.\n", thread_id, k, event_no, events[event_no].available_seats);
    }
    pthread_rwlock_unlock(&events[event_no].rw_lock);
}

void cancel_ticket(int thread_id, vector<Booking>& thread_bookings, unsigned int& seed) {
    if (thread_bookings.empty()) {
        printf("Thread:%d [CANCEL-FAIL] No existing bookings to cancel.\n", thread_id);
        return;
    }
    
    int index = rand_r(&seed) % thread_bookings.size();
    int event_no = thread_bookings[index].event_no;
    int k = thread_bookings[index].seats_booked;

    // WRITER: Exclusive lock prevents race conditions during cancellation
    pthread_rwlock_wrlock(&events[event_no].rw_lock);
    events[event_no].available_seats += k;
    printf("Thread:%d [CANCEL] Successfully cancelled %d seats for Event %d. Seats left: %d\n", thread_id, k, event_no, events[event_no].available_seats);
    pthread_rwlock_unlock(&events[event_no].rw_lock);

    thread_bookings.erase(thread_bookings.begin() + index);
}

void* thread_function(void* t_id) {
    int thread_id = *(int*)t_id;
    free(t_id);
    
    time_t start = time(NULL);
    vector<Booking> thread_bookings;
    unsigned int seed = time(NULL) + thread_id;

    while (time(NULL) - start < runtime) {
        // Admission Control: Wait for an available slot in the system
        sem_wait(&max_query_semaphore);

        int event_no = rand_r(&seed) % num_events;
        int query_type = rand_r(&seed) % 3; // 0: Read, 1: Book, 2: Cancel

        if (query_type == 0) {
            read_query(event_no, thread_id);
        } else if (query_type == 1) {
            book_ticket(event_no, thread_id, thread_bookings, seed);
        } else {
            cancel_ticket(thread_id, thread_bookings, seed);
        }

        // Release capacity slot for the next pending query
        sem_post(&max_query_semaphore);
        
        sleep((rand_r(&seed) % 2) + 1); // Simulate time taken by a user
    }

    pthread_exit(NULL);
}

int main() {
    printf("Enter the number of events: ");
    if(scanf("%d", &num_events) != 1) return 1;
    printf("Enter the capacity of auditorium: ");
    if(scanf("%d", &capacity) != 1) return 1;
    printf("Enter the number of worker threads: ");
    if(scanf("%d", &num_threads) != 1) return 1;
    printf("Enter the maximum number of concurrent active queries: ");
    if(scanf("%d", &max_queries) != 1) return 1;
    printf("Enter the total running time in seconds: ");
    if(scanf("%d", &runtime) != 1) return 1;

    // Initialize POSIX Semaphore for system-wide admission control
    sem_init(&max_query_semaphore, 0, max_queries);

    // Initialize Events and their individual Readers-Writers locks
    events.resize(num_events);
    for (int i = 0; i < num_events; i++) {
        events[i].available_seats = capacity;
        pthread_rwlock_init(&events[i].rw_lock, NULL);
    }

    pthread_t threads[num_threads];

    for (int i = 0; i < num_threads; i++) {
        int* thread_id = (int*)malloc(sizeof(int));
        *thread_id = i;
        if (pthread_create(&threads[i], NULL, thread_function, thread_id)) {
            printf("Error while creating threads.\n");
            exit(-1);
        }
    }

    for (int i = 0; i < num_threads; i++) {
        pthread_join(threads[i], NULL);
    }
    
    printf("\n--- All threads have finished. ---\n");
    printf("Final Booked Seats Summary:\n");
    for (int i = 0; i < num_events; i++) {
        printf("Event %d: Booked seats %d\n", i, capacity - events[i].available_seats);
        pthread_rwlock_destroy(&events[i].rw_lock);
    }

    sem_destroy(&max_query_semaphore);
    return 0;
}
