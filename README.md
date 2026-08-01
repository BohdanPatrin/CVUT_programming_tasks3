# Multithreaded Asynchronous Message Processor

## Overview
This project is a high-performance, multithreaded C++ application designed to asynchronously receive, reassemble, and process fragmented data streams. Developed as part of an Operating Systems curriculum, the system is built to handle strict memory and execution time limits (evaluated via the Progtest platform). It demonstrates advanced thread management, synchronization, and algorithmic optimization.

## Key Features
*   **Asynchronous I/O Processing:** Handles fragmented data arriving concurrently from multiple simulated network receivers.
*   **Thread Synchronization:** Implements thread-safe data structures using POSIX/C++11 mutexes and condition variables to prevent deadlocks and race conditions.
*   **Custom Thread Pool Architecture:** 
    *   **Receiver Threads:** Continuously poll for incoming fragments without blocking computations.
    *   **Worker Threads:** Wait for complete messages to be assembled, then perform CPU-bound calculations in parallel.
    *   **Transmitter Threads:** Safely queue and transmit processed results back to the interface without bottlenecking the workers.
*   **Algorithmic Optimization:** Solves boolean prefix expressions to evaluate valid problem states. The computation algorithm is optimized to run within an $O(n^3)$ time complexity limit.

## Code Highlight: Worker Thread Synchronization
The worker threads utilize a consumer model, safely waiting for dynamically allocated `CMsgSerializer` tasks. This ensures zero CPU cycle waste (no busy-waiting) when the job queue is empty.

```cpp
void CSentinelHacker::workerLoop() {
    while (true) {
        std::shared_ptr<CMsgSerializer> myJob;

        {
            std::unique_lock<std::mutex> lock(mtx);

            // Sleep safely until a job arrives or the system is stopping
            cv_workers.wait(lock, [this]() {
                return !jobQueue.empty() || isStopped;
            });

            // Clean exit condition
            if (isStopped && jobQueue.empty())
                return;

            myJob = jobQueue.front().serializer;
            jobQueue.front().threadsRemaining--;

            if (jobQueue.front().threadsRemaining == 0)
                jobQueue.pop();
        }
        
        // Execute heavy computation outside the critical section
        myJob->solve();
    }
}
```

## System Architecture
1.  **Fragment Reception:** `CReceiver` instances asynchronously deliver 64-bit fragments containing a message ID, fragment count, and payload.
2.  **Assembly & Validation:** Fragments are grouped by ID. Once all fragments for a message arrive, they are validated and passed to a deserializer (`CMsgSerializer`).
3.  **Parallel Computation:** The deserializer yields bitfields representing boolean operands. Worker threads parse these bitfields to find all valid prefix expressions using AND, OR, and XOR operators that evaluate to `true`.
4.  **Transmission:** Successfully calculated permutations are handed off to `CTransmitter` threads for sequential, thread-safe reporting.

## Tech Stack
*   **Language:** Standard C++20
*   **Concurrency:** `<thread>`, `<mutex>`, `<condition_variable>`
*   **Build System:** Make
*   **Environment:** Cross-platform (includes pre-compiled static libraries for Linux, macOS, and Windows/MinGW64)

## How to Build and Run
This project includes standard Makefiles for different environments. 

**For Linux/macOS:**
```bash
make -f Makefile
./test.out
```

**For Windows (MinGW64):**
```bash
mingw32-make -f Makefile.mingw
test.exe
```
## Project Structure
*   `solution.cpp` - Core implementation of the `CSentinelHacker` class, thread lifecycle management, and synchronization logic.
*   `sample_tester.cpp` - The testing framework simulating the asynchronous network environment.
*   `common.h` / `progtest_solver.h` - Interfaces and pre-defined structures for the testing environment.
*   Architecture Folders (e.g., `i686-w64-mingw32`, `x86_64-linux-gnu`) - Contain the necessary `libprogtest_solver.a` static libraries for local compilation across different environments.
