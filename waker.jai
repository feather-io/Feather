// Implement the Waker: the signal mechanism that moves a sleeping Future
// back onto the Executor's run-queue when the Reactor detects readiness.
//
// Architecture:
//   Feather's async model has three actors that interact through a Future:
//
//     1. The Executor (worker threads) – polls Futures and transitions
//        schedule_state between QUEUED → RUNNING → IDLE/QUEUED.
//
//     2. The Reactor (I/O event loop, typically kqueue/epoll) – receives
//        kernel readiness notifications and calls wake() to re-schedule the
//        sleeping Future.
//
//     3. The Waker (this file) – the shared interface between the Reactor
//        and the Executor.  It is stored in `context.waker` by the worker
//        thread before invoking `poll`, making it accessible to any leaf
//        operation that needs to register itself for re-waking.
//
//   The Waker deliberately holds only a *Future (not any queue reference)
//   so that the Reactor does not need to know which thread or queue the
//   Future came from.  All routing information is encoded inside the Future
//   itself via `fut.executor`.
//
#import "Thread";

//
// Bundle the Future pointer and its owning Executor into a single waker token.
//
// Architecture:
//   `fut`  – the exact Future to re-enqueue when the awaited event fires.
//   `exec` – the Executor that owns this Future's run-queue.  Required
//             because spawn() needs the Executor's mutex to safely call
//             add_work!() on the underlying Thread_Group.  We cannot derive
//             the Executor from the Thread_Group pointer at waker time
//             because the cast trick (group == &exec.group at offset 0)
//             is only valid from inside worker_proc, not from the Reactor.
//
Waker :: struct {
    fut:  *Future;
    exec: *Executor; // Changed back to Executor so we can use its mutex
}

// Inject the current Waker into the Jai context so that any async leaf
// operation running on a worker thread can read it without an explicit
// parameter thread-through.
#add_context waker: *Waker;

// Re-schedule `fut` onto its Executor so the worker pool polls it again.
//
// Architecture:
//   This function is the *only* legal way for the Reactor (or any code
//   outside the Executor) to signal that a Future is ready for another
//   poll.  It must be safe to call from any thread at any time, including
//   concurrently from multiple Reactor callbacks for the same Future.
//
//   A plain store is not sufficient here because two threads could both
//   observe IDLE, both decide to call spawn(), and enqueue the Future
//   twice – corrupting the Thread_Group queue.  A CAS loop solves this:
//   only the thread whose CAS succeeds takes the resulting action; all
//   others retry and discover that someone else already handled it.
//
//   The loop handles every possible schedule_state:
//
//     IDLE (0) → QUEUED (1):
//       The Future is parked and not in any queue.  The winning CAS
//       transitions to QUEUED and calls spawn() to inject it into the
//       Executor's Thread_Group.  Losers retry and will see QUEUED on
//       the next read, falling into the QUEUED branch and returning early.
//
//     QUEUED (1) → (no transition):
//       The Future is already waiting in the run-queue.  Nothing to do;
//       a second enqueue would corrupt the queue or double-poll.  Return
//       immediately.
//
//     RUNNING (2) → RUNNING_AND_WOKEN (3):
//       A worker thread is *currently* inside `poll`.  We must not call
//       spawn() yet – the work item is not free to be re-added while the
//       worker holds it.  Instead, the winning CAS marks the Future as
//       "woken during execution".  When worker_proc finishes polling, its
//       own CAS loop detects RUNNING_AND_WOKEN and immediately transitions
//       to QUEUED + spawn(), so the Future gets re-polled without any
//       extra wake() call.  This prevents the double-enqueue race entirely.
//
//     RUNNING_AND_WOKEN (3) → (no transition):
//       A previous wake() already set this flag while the worker was
//       running.  The worker will self-requeue on exit.  A second mark
//       is redundant; return immediately.
//
wake :: (fut: *Future) {
    if !fut return;

    Atomic :: #import "Atomics";

    // Transition from IDLE (0) to QUEUED (1).
    // Or if RUNNING (2) to RUNNING_AND_WOKEN (3).
    while true {
        st := Atomic.atomic_read(*fut.schedule_state);
        if st == .QUEUED {
            // Already QUEUED
            break;
        }
        if st == .IDLE {
            _, comp_val := compare_and_swap(*fut.schedule_state, .IDLE, .QUEUED);
            if comp_val  == .IDLE {
                spawn(fut.executor, fut);
                break;
            }
        } else if st == .RUNNING {
            _, comp_val := compare_and_swap(*fut.schedule_state, .RUNNING, .RUNNING_AND_WOKEN);
            if comp_val  == .RUNNING {
                // The worker thread will see it was woken and re-queue it automatically
                break;
            }
        } else if st == .RUNNING_AND_WOKEN {
            // Already RUNNING_AND_WOKEN
            break;
        }
    }
}
