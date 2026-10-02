# Multithreaded Event Reservation System

A high-concurrency ticket booking engine built in C++ using POSIX threads. The system simulates real-time user traffic with robust synchronization mechanisms to ensure data consistency and prevent race conditions.

## Key Technical Features
*   **Per-Event Readers-Writers Locking:** Implemented `pthread_rwlock_t` on a per-event basis. This allows multiple users to query seat availability simultaneously while strictly locking out reads during exclusive write operations (booking or canceling).
*   **Semaphore-Based Admission Control:** Utilizes POSIX semaphores (`<semaphore.h>`) to enforce a strict system-wide cap on concurrent active queries, preventing server overload during high-traffic simulations.
*   **Deadlock-Free State Management:** Safely manages real-time seat capacity across asynchronous bookings and cancellations. The locking hierarchy guarantees that threads never enter a circular dependency state.

## Execution
Compile and run using GCC with the pthread library:
```bash
g++ reservation_system.cpp -o reservation_system -pthread
./reservation_system
