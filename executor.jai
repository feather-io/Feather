#import "Thread";



// Hold the Jai `Thread_Group` (a work-stealing thread pool) together with the
// mutex that makes `add_work` safe for concurrent callers.
//
// **Memory-layout contract (C-style pointer aliasing trick)**
//
// `group` is intentionally placed as the **first member** of this struct.
// This makes the address of an `Executor` identical to the address of its
// embedded `Thread_Group`. Consequently, anywhere the runtime receives a
// `*Thread_Group` from the OS-level callback (e.g. `worker_proc`), it can
// recover the owning `*Executor` with a plain `cast(*Executor) group` - no
// offset arithmetic required.
//
// The same layout contract is extended one level up: `exec` is the **first
// member** of `Feather_Runtime`. That means a single `*Thread_Group` pointer
// can be cast all the way up to `*Feather_Runtime`, enabling worker threads
// to reconstruct the full runtime context purely from the pointer the OS
// thread library hands them.
//
// Summary of the cast chain:
//   `*Thread_Group`  →  `cast(*Executor)`        (group is member[0])
//   `*Thread_Group`  →  `cast(*Feather_Runtime)`  (exec  is member[0] of Feather_Runtime)
Executor :: struct {
    // The OS-level, work-stealing thread pool.
    //
    // **MUST remain the first member.** Moving it, or adding any field before
    // it, silently breaks every pointer cast in `worker_proc` and
    // `execute_future_inline`. There is no compile-time guard against this;
    // the invariant must be maintained by discipline.
    group: Thread_Group;

    // Serialises calls to `Thread_Group.add_work`, which is **not** internally
    // thread-safe.
    //
    // Without this mutex, two threads that both call `spawn` simultaneously
    // can corrupt the work queue's internal linked list, leading to lost
    // futures, double-frees, or silent hangs. Every call-site that enqueues
    // work must hold this lock for the duration of `add_work`.
    mutex: Mutex;
}

// Initialise the executor and start the worker threads.
//
// Disables `Thread_Group` logging (Feather uses its own diagnostics), then
// creates and immediately starts `num_threads` OS threads with work-stealing
// enabled. After this call returns, the thread pool is live and ready to
// accept futures via `spawn`.
init_executor :: (e: *Executor, num_threads: s32) {
    init(*e.mutex);

    e.group.logging = false;
    init(*e.group, num_threads, worker_proc, enable_work_stealing = true);
    start(*e.group);
}

// Enqueue `fut` onto the executor's thread pool for asynchronous execution.
//
// `Thread_Group.add_work` is **not** thread-safe; concurrent calls from
// multiple Reactor threads or worker threads would race on the queue's
// internal state. The mutex serialises all enqueue operations so that only
// one caller modifies the queue at a time.
//
// The `fut.executor` back-pointer is set before locking so that the future
// already knows which executor owns it by the time any worker thread picks
// it up.
spawn :: (e: *Executor, fut: *Future) {
    fut.executor = e;
    lock(*e.mutex);
    add_work(*e.group, fut);
    unlock(*e.mutex);
}

// Enqueue `fut` using the executor stored in the calling thread's context.
//
// A convenience wrapper for code running inside a worker or reactor thread
// where `context.feather` is already populated. Semantics are identical to
// `spawn`; see that procedure for the thread-safety rationale.
spawn_injected :: (fut: *Future) {
    fut.executor = *context.feather.exec;
    lock(*context.feather.exec.mutex);
    add_work(*context.feather.exec.group, fut);
    unlock(*context.feather.exec.mutex);
}

// Execute one unit of async work on a worker thread.
//
// This is the callback registered with `Thread_Group`. The OS thread library
// calls it with the raw `*Thread_Group` pointer and the `*void` work item
// that was previously enqueued via `add_work`. The procedure carries out
// **seven distinct responsibilities** to drive a `Future` through one
// polling cycle:
//
// 1. **Recover `*Executor` via struct-cast.**
//    The `group` argument is the address of `Executor.group`. Because `group`
//    is member[0], `cast(*Executor) group` yields the owning `Executor`
//    with zero byte offset.
//
// 2. **Inject `Waker` and `Feather_Runtime` into `context`.**
//    Worker OS threads start with a blank `context`. Before polling, we push
//    a new context that contains:
//    - `context.waker`   - the `Waker` that async I/O drivers use to
//      re-queue this future when data is ready.
//    - `context.feather` - the full `*Feather_Runtime`, recovered by casting
//      `group` one level further up (valid because `exec` is member[0] of
//      `Feather_Runtime`).
//
// 3. **Atomically transition `schedule_state` to `RUNNING`.**
//    A CAS spin-loop moves the state from whatever it currently is (typically
//    `QUEUED`) to `RUNNING`, establishing that this thread now owns the
//    future. The loop retries on CAS failure (another core won the race)
//    until the transition succeeds.
//
// 4. **Poll the future.**
//    Calls `fut.poll(fut)`, the vtable-style function pointer on `Future`.
//    The future executes until it either completes or encounters an async
//    operation that is not yet ready, at which point it returns `PENDING`.
//
// 5. **On `READY` - free the future.**
//    The future has finished. Its heap allocation is released immediately.
//    NOTE: If cancellation is ever added, the future may already have been
//    freed by the cancellation path before this point. See the `@FIXME`
//    comment in the source for the use-after-free risk.
//
// 6. **On `PENDING` - CAS-based re-queue decision.**
//    After polling, the future's state must be transitioned back:
//    - `RUNNING → IDLE`:  The future is parked; a `Waker` will re-queue it
//      later when the awaited I/O event arrives.
//    - `RUNNING_AND_WOKEN → QUEUED` + immediate `spawn`: The `Waker` fired
//      *during* the poll (on a different thread). To avoid a missed wake-up,
//      we detect this state and immediately re-enqueue the future so no
//      reactor cycle is wasted.
//    Both branches are CAS spin-loops to handle races with concurrent wakers.
//
// 7. **Local GC of `Work_Entry` nodes.**
//    The `Thread_Group` internally allocates a `Work_Entry` node per unit of
//    work. Completed entries accumulate in a per-thread `completed` list.
//    Rather than calling the expensive global `get_completed_work()` after
//    every task, we batch-free nodes locally once the list exceeds 16
//    entries. This keeps the hot path allocation-free for the common case.
worker_proc :: (group: *Thread_Group, thread: *Thread, work: *void) -> Thread_Continue_Status {
    fut := cast(*Future) work;

    // Responsibility 1: recover the owning Executor.
    // `group` is a `*Thread_Group`. Because `Thread_Group` is the first field
    // of `Executor` (at byte offset 0), this cast is a legal pointer alias -
    // no arithmetic needed.
    exec := cast(*Executor) group;

    waker: Waker;
    waker.exec = exec;
    waker.fut  = fut;

    push_context,defer_pop; {
        // Responsibility 2: inject runtime context into the worker thread.
        context.waker = *waker;

        // `Executor` is the first field of `Feather_Runtime`, so
        // `cast(*Feather_Runtime) group` is safe: the pointer still lands at
        // the base address of the `Feather_Runtime` allocation.
        context.feather = cast(*Feather_Runtime) group;

        Atomic :: #import "Atomics";

        // Responsibility 3: atomically claim ownership of the future.
        // A spin-loop CAS ensures that exactly one worker transitions the
        // state to RUNNING. If the CAS fails (another core beat us), we read
        // the fresh state and retry. IDLE (0) is the initial state for
        // futures started outside normal scheduling (e.g. the acceptor loop).
        while true {
            st := Atomic.atomic_read(*fut.schedule_state);
            _, comp_val := compare_and_swap(*fut.schedule_state, st, .RUNNING);
            if comp_val == st {
                break;
            }
        }

        // Responsibility 4: drive the future forward by one async step.
        state := fut.poll(fut);

        if state == .READY {

            // Responsibility 5: release the future's memory on completion.
            //
            // @FIXME: we might want to add cancellation in the future
            // if we do that this `fut` will be freed way before this statement
            // executes, so the below line becomes use-after-free panic.
            //
            // [Handle this when implementing cancellation]
            free(fut);
        } else {

            // Responsibility 6: determine the future's fate after a PENDING poll.
            //
            // Two possible transitions (both implemented as CAS spin-loops to
            // handle concurrent wakers):
            //
            //   RUNNING           → IDLE            : park; waker re-queues later.
            //   RUNNING_AND_WOKEN → QUEUED + spawn  : waker fired mid-poll;
            //                                         re-queue immediately to
            //                                         avoid a missed wake-up.
            while true {
                st := Atomic.atomic_read(*fut.schedule_state);
                if st == .RUNNING {
                    _, comp_val := compare_and_swap(*fut.schedule_state, .RUNNING, .IDLE);
                    if comp_val == .RUNNING {
                        break;
                    }
                } else if st == .RUNNING_AND_WOKEN {
                    _, comp_val := compare_and_swap(*fut.schedule_state, .RUNNING_AND_WOKEN, .QUEUED);
                    if comp_val == .RUNNING_AND_WOKEN {
                        spawn(exec, fut);
                        break;
                    }
                } else {
                    break;
                }
            }
        }
    }

    // Responsibility 7: local GC of Work_Entry nodes.
    //
    // `Thread_Group` heap-allocates a `Work_Entry` wrapper for every unit of
    // work and places completed ones on a per-thread list. Calling the
    // global `get_completed_work()` acquires a shared lock on every call -
    // prohibitively expensive on a hot executor. Instead we drain the local
    // list in bulk once it grows beyond 16 nodes: lock once, steal the
    // entire list, then walk and free outside the lock.
    list := *thread.worker_info.completed;
    if list.count > 16 {
        lock(*list.mutex);
        node := list.first;
        list.first = null;
        list.last = null;
        list.count = 0;
        unlock(*list.mutex);

        while node {
            next := node.next;
            free(node,, thread.worker_info.group.allocator);
            node = next;
        }
    }

    return .CONTINUE;
}

// ------------------------------------------------------------------ //
// *** INLINE FAST-PATH: execute a future on the calling thread    *** //
// ------------------------------------------------------------------ //

// Poll `fut` immediately on the calling thread, bypassing the `Thread_Group`
// queue entirely.
//
// **Motivation - eliminating per-event heap allocations**
//
// The normal `spawn` path allocates a `Work_Entry` node on the heap for
// every future that is enqueued. On a high-throughput HTTP server that
// handles tens of thousands of I/O events per second, those allocations add
// measurable latency and GC pressure. When the Reactor thread itself detects
// that a future is ready to make progress (e.g. a socket just became
// readable), it can call `execute_future_inline` instead of `spawn`. The
// future runs synchronously on the Reactor thread, the result is processed,
// and no `Work_Entry` allocation ever occurs.
//
// **Concurrency contract**
//
// The function first races to claim the future via CAS, mirroring the state
// machine in `worker_proc`:
//
//   - `IDLE` or `QUEUED` → `RUNNING`  : we win; proceed to poll inline.
//   - `RUNNING`          → `RUNNING_AND_WOKEN` : a worker already owns the
//     future; signal it to re-queue after its current poll by setting the
//     woken flag, then return. The worker's post-poll CAS loop (step 6 in
//     `worker_proc`) will see `RUNNING_AND_WOKEN` and re-enqueue.
//   - `RUNNING_AND_WOKEN` : already signalled; nothing to do.
//
// After a successful inline poll, the post-poll state cleanup is identical
// to `worker_proc`: `READY` frees the future; `PENDING` applies the same
// `RUNNING → IDLE` or `RUNNING_AND_WOKEN → QUEUED + spawn` CAS logic.
execute_future_inline :: (fut: *Future) {
    if !fut return;

    Atomic :: #import "Atomics";

    // Race to claim the future for inline execution.
    // If another thread is already polling it, escalate to RUNNING_AND_WOKEN
    // so the worker re-queues after its current poll completes.
    can_run := false;
    while true {
        st := Atomic.atomic_read(*fut.schedule_state);
        if st == .IDLE || st == .QUEUED {
            _, comp := compare_and_swap(*fut.schedule_state, st, .RUNNING);
            if comp == st { can_run = true; break; }
        } else if st == .RUNNING {
            _, comp := compare_and_swap(*fut.schedule_state, .RUNNING, .RUNNING_AND_WOKEN);
            if comp == .RUNNING { break; }
        } else if st == .RUNNING_AND_WOKEN {
            break;
        }
    }

    if can_run {
        waker: Waker;
        waker.exec = fut.executor;
        waker.fut = fut;

        push_context {
            context.waker = *waker;
            // Recover the full runtime via the same struct-cast chain used in
            // `worker_proc`: Executor is member[0] of Feather_Runtime.
            context.feather = cast(*Feather_Runtime) fut.executor;

            state := fut.poll(fut);

            if state == .READY {
                free(fut);
            } else {
                // Mirror the post-poll CAS logic from worker_proc (step 6).
                // RUNNING → IDLE parks the future.
                // RUNNING_AND_WOKEN → QUEUED hands it off to the thread pool,
                // because the Reactor thread cannot block indefinitely on a
                // PENDING future that needs more work.
                while true {
                    st := Atomic.atomic_read(*fut.schedule_state);
                    if st == .RUNNING {
                        _, comp := compare_and_swap(*fut.schedule_state, .RUNNING, .IDLE);
                        if comp == .RUNNING { break; }
                    } else if st == .RUNNING_AND_WOKEN {
                        _, comp := compare_and_swap(*fut.schedule_state, .RUNNING_AND_WOKEN, .QUEUED);
                        if comp == .RUNNING_AND_WOKEN {
                            spawn(fut.executor, fut);
                            break;
                        }
                    } else {
                        break;
                    }
                }
            }
        }
    }
}
