// Define the core Future primitive for Feather's async execution model.
//
// Architecture:
//   A Future represents a single pausable computation.  Rather than using
//   OS threads (one-thread-per-task), Feather describes every asynchronous
//   operation as a heap-allocated Future that can be polled incrementally.
//   The Executor drives Futures by calling their `poll` function pointer.
//   When a Future cannot make progress (e.g. waiting for I/O), it returns
//   PENDING and suspends itself.  The Reactor later calls wake() to re-enqueue
//   it so the Executor can poll it again.
//
//   This model is inspired by Rust's Future trait (zero-cost async/await via
//   state-machine compilation), but Feather takes a simpler, more explicit
//   approach: each Future is a manually heap-allocated struct with a single
//   `poll` function pointer.  There is no monomorphization – every Future is
//   type-erased through that single pointer, which keeps the Executor and
//   Reactor generic and free of compile-time specialisation overhead.
//
#import "Basic";

//
// Track the scheduling lifecycle of a Future atomically.
//
// Architecture:
//   The state machine below is the heart of the double-enqueue prevention
//   mechanism.  All transitions are performed with compare-and-swap so that
//   the Reactor thread and any number of worker threads can concurrently
//   observe and mutate the state without holding a mutex around the hot path.
//
//   Legal transitions (see executor.jai and waker.jai for the CAS code):
//
//     IDLE ──(wake)──────────────► QUEUED ──(worker picks up)──► RUNNING
//       ▲                                                            │
//       │                  wake() called while running:             │
//       │              RUNNING ──(CAS)──► RUNNING_AND_WOKEN         │
//       │                                        │                  │
//       └──(poll returns PENDING, no wake)────────┘                 │
//                                                                   │
//       worker finishes, RUNNING_AND_WOKEN detected:               │
//       RUNNING_AND_WOKEN ──(CAS)──► QUEUED ──(spawn)──────────────┘
//
Future_Schedule_State :: enum s32 {
    // The Future is parked: not in any run-queue, waiting for the Reactor.
    IDLE            :: 0;
    //
    // Represent a Future that has been submitted to the Executor's work queue
    // but not yet picked up by a worker thread.
    QUEUED          :: 1;
    //
    // Indicate that a worker thread is actively executing `poll` right now.
    // No second enqueue must happen while this state is active.
    RUNNING         :: 2;
    //
    // Record that wake() fired concurrently while the worker was inside `poll`.
    // The worker will detect this on exit and self-re-queue, avoiding the
    // need for the Reactor to enqueue it again (double-enqueue prevention).
    RUNNING_AND_WOKEN :: 3;
}

//
// Represent a single unit of pausable async work managed by the Executor.
//
// Architecture:
//   `poll` is a raw function pointer rather than a vtable or closure.
//   The caller (worker_proc in executor.jai) casts the opaque *void work item
//   back to *Future and dispatches through this pointer.  Concrete async
//   operations (TCP reads, timer sleeps, etc.) embed Future as their *first*
//   struct member so the same pointer serves as both the Future header and
//   the concrete type – a manual, zero-cost form of type erasure.
//
//   `schedule_state` is read and written exclusively through atomic operations
//   (see Future_Schedule_State above).  It must never be touched under a plain
//   load/store.
//
//   `executor` is set by spawn() when the Future is first enqueued and is
//   stored here so that wake() can re-enqueue to the correct Executor without
//   any additional context (the Waker only holds a *Future).
//
Future :: struct {
    poll:           (*Future) -> Poll_State;
    schedule_state: Future_Schedule_State;
    executor:       *Executor;
}

//
// Enumerate every outcome that a single poll() invocation can produce.
//
// Architecture:
//   `poll` must return one of these values each time it is called:
//
//   READY   – The computation finished successfully.  The Executor frees the
//             Future immediately after poll returns.  Do not access `fut`
//             after returning READY.
//
//   PENDING – The computation is waiting for an external event (I/O
//             completion, timer expiry, etc.).  The Future has registered a
//             Waker via context.waker so the Reactor can call wake() later.
//             The Executor transitions schedule_state to IDLE and stops
//             polling until wake() fires.
//
//   ERROR   – The computation encountered an unrecoverable error.  The
//             Executor treats this identically to READY for lifetime purposes
//             (the Future is freed); higher-level protocol code is expected
//             to have already recorded or surfaced the error before returning.
//
//   EOF     – The underlying connection or stream was closed by the remote
//             peer.  Signals a clean, expected termination rather than an
//             error condition so that callers can distinguish graceful shutdown
//             from mid-stream failures.
//
Poll_State :: enum {
    READY;
    PENDING;
    ERROR;
    EOF;
}
