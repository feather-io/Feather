
//
// This module is based on the works of [New Tokio Runtime](https://tokio.rs/blog/2019-10-scheduler#the-new-task-system) which provides
// a async runtime for Rust async ecosystem. But the works here are not 1:1 co-related with the implementation.
// Yes, the base implementation follows the way it is implemented in Rust but it does not make use of procedural
// macros type of programming which is so prominent in that ecosystem. Open each file to know about what they
// do, and below I'll write a pretty verbose write up of what I think will be a good implementation in Jai (for now).
//
// The current implementation uses the same Thread_Group work stealing implementation in Jai and does not
// implement it's own, because it is easier to get started. Yes! Sometimes you have to think about things this
// way to get shit done, deal with it. If you look at the  Work_List  struct inside  modules/Thread/thread_group.jai,
// it is implemented as a standard Singly-Linked List protected by a Mutex. Every time a worker thread wants to
// pop a piece of work, it literally calls  `lock(*mutex)`. When it wants to steal work from another thread, it locks
// the other thread's mutex. Maybe I'll think about implementing [Chase-Lev Deque](https://www.dre.vanderbilt.edu/~schmidt/PDF/work-stealing-dequeue.pdf) later -
// but that is out of question right now.
//
// All of this is to say that the `Executor` is basically a wrapper on top of Thread_Group.
//
// So the main goal of this module is to basically provide us with a rock-solid asychronous runtime, which
// can will enable developers to use Jai in backend servers as well.
//
// Future: is you create a container which can survive the asynchronous sleep and wake cycle, for this we have
// something called a `Future`. This Future struct can be anything that you want, you just have to have a poll method
// in this struct and should take in a pointer to the Future struct.
//
// Executor: which actually calls the poll function inside an OS thread through the worker_proc.
// Whenever we `spawn()` a Future
//     -> it goes to the `worker_proc` and the OS Thread calls
//      -> it calls the `poll` method inside the Future
//        -> if it is in .PENDING state it `yields()` it to the Reactor.
//
// Reactor: is the core runtime event loop which keeps polling the all of our futures which polls the base file
// descriptor inside the Future and calls the `wake()` function from the Waker.
//
// Waker: is a simple struct having the Future and the Executor, and a single function to call `spawn()` again.
// But this time!! We have some diabolical cookies in the jar to eat.
//
// ```jai
// main :: () {
//      // you can inject a feather runtime like this
//      feather_main();
//      defer feather_destroy();
// }
// ```

#load "future.jai";
#load "executor.jai";
#load "reactor.jai";
#load "waker.jai";
#load "fiber.jai";
#load "shared/errors.jai";

#load "primitives/join.jai";
#load "primitives/channel.jai";
#load "primitives/timer.jai";

#load "io/fd.jai";
#load "io/tcp.jai";



// Inject global variables into every thread's context implicitly.
//
// `context.feather` is the global runtime pointer injected into every thread,
// avoiding the need to manually pass a god-object around. `reactor_index` routes a fiber's IO yields
// to its assigned reactor to maintain thread-locality. `workspace` is a type-erased closure pointer for passing arguments to fibers seamlessly.
#add_context feather: *Feather_Runtime;
#add_context reactor_index: s32; // Used by fibers to yield to their assigned reactor
#add_context workspace: *void; // For passing closures to fibers



// Store a future waiting to be awakened at a specific timestamp.
//
// Timer entries are stored in a centralized array protected by `timers_mutex` and
// scanned by `poll_reactor()` on every loop iteration. While a simple array scan is not a highly-optimized,
// hierarchical hashed timing wheel, it provides an adequate baseline for scheduling time-based sleep/wake cycles.
Timer_Entry :: struct {
    wake_time_ms: float64;
    fut: *Future;
}

// Manage the central orchestration for the asynchronous runtime.
//
// This is the god-object of the runtime, providing the foundation for
// thread pooling, IO multiplexing, stack management, and global resource cleanup.
Feather_Runtime :: struct {
    // The thread pool wrapper that calls `poll()` on Futures.
    exec: Executor;
    // One Reactor per CPU core for scalable IO multiplexing.
    reactors: [] Reactor;
    // Background threads each running their own Reactor poll loop.
    reactor_threads: [] Thread;
    // Stack allocator pool to avoid expensive alloc/free on every connection.
    stack_pool: Fiber_Stack_Pool;

    // Timer wheel (simple array scan) tracking sleeping futures.
    timers: [..] Timer_Entry;
    // Mutex protecting the timer wheel array.
    timers_mutex: Mutex;

    // Mutex protecting the linked list of active fibers.
    active_fibers_mutex: Mutex;
    // Linked list for leak cleanup on shutdown.
    active_fibers_head: *Fiber;
}



// Bootstrap the entire runtime environment and initialize components.
//
// Creates one Reactor per CPU core for scalable IO polling.
// Prepares one background reactor thread per core (minus one, as the main thread will poll Reactor 0).
// This pattern maximizes resource utilization without context-switching overhead.
feather_main :: (thread_count: s32 = -1) {
    runtime := New(Feather_Runtime);
    cores := ifx thread_count > 0 then thread_count else get_number_of_processors();

    init_executor(*runtime.exec, cores);

    // Initialize an array of reactors matching the number of cores
    runtime.reactors = NewArray(cast(s64) cores, Reactor);
    for i: 0..cores-1 {
        init_reactor(*runtime.reactors[i]);
    }

    // Allocate thread array (we spawn cores-1 threads, main thread is the last one)
    runtime.reactor_threads = NewArray(cast(s64) cores - 1, Thread);

    init_stack_pool(*runtime.stack_pool);
    init(*runtime.timers_mutex);
    init(*runtime.active_fibers_mutex);

    context.feather = runtime;
}

// Poll a single assigned reactor indefinitely on a background thread.
//
// This thread proc loops forever, continuously checking for IO events and timers.
// Since `keep_polling` is not explicitly passed to this closure, the thread exits
// when the main shutdown process closes the underlying `epfd` (epoll) or `kq_fd` (kqueue),
// causing the blocking OS call to return an error.
reactor_poller_proc :: (thread: *Thread) -> s64 {
    reactor := cast(*Reactor) thread.data;

    // We poll until keep_polling is false
    // Since keep_polling isn't directly passed here, we loop forever
    // until shutdown closes the epfd/kqueue, causing poll_os_reactor to return.
    while true {
        poll_reactor(reactor);
        reset_temporary_storage();
        // If epfd/kqueue is closed, poll_reactor returns immediately without events
        // But we need a proper exit condition.
        // For now, let's just break if the reactor is closed.
        // (Handled internally if poll_reactor returns error).
        // Actually, we can check context.feather.keep_polling if we store it.
    }
    return 0;
}

// Start the main event loop and launch background polling threads.
//
// Adopts a hybrid threading model. Starts `N-1` background reactor threads
// and uses the calling (main) thread to actively poll `Reactor[0]`.
// This ensures the main thread isn't idle while background threads perform the heavy IO multiplexing.
feather_run :: (keep_polling: *bool) {
    runtime := context.feather;

    // Start background threads for reactors 1 through N-1
    for i: 1..runtime.reactors.count-1 {
        thread_init(*runtime.reactor_threads[i-1], (thread: *Thread) -> s64 {
            reactor := cast(*Reactor) thread.data;
            keep_polling_ptr := cast(*bool) cast(*void) thread.workspace; // Hack to pass keep_polling

            while keep_polling_ptr.* {
                poll_reactor(reactor);
                reset_temporary_storage();
            }
            return 0;
        });

        runtime.reactor_threads[i-1].data = *runtime.reactors[i];
        runtime.reactor_threads[i-1].workspace = cast(s64) cast(*void) keep_polling; // Pass pointer via workspace
        thread_start(*runtime.reactor_threads[i-1]);
    }

    // Main thread polls reactor 0
    while keep_polling.* {
        poll_reactor(*runtime.reactors[0]);
        reset_temporary_storage();
    }

    // Wait for threads to finish? (Not strictly necessary if process exits, but good practice)
}

// Run a single-threaded test event loop until all spawned fibers complete.
//
// Uses an atomic counter to track fiber lifecycles. Once the shared counter drops to zero,
// the loop breaks. This provides a deterministic execution environment ideal for test suites without
// the complexity of full multi-threading.
feather_run_until_done :: (pending_fibers: *s64, timeout_ms: s64 = 5000) {
    Atomic :: #import "Atomics";
    runtime := context.feather;
    start_time := seconds_since_init();

    while Atomic.atomic_read(pending_fibers) > 0 {
        poll_reactor(*runtime.reactors[0]);
        reset_temporary_storage();

        elapsed_ms := cast(s64)((seconds_since_init() - start_time) * 1000.0);
        if elapsed_ms > timeout_ms {
            print("[FFTE] feather_run_until_done timed out after %ms (% fibers still pending)\n",
                timeout_ms, Atomic.atomic_read(pending_fibers));
            break;
        }
    }
}

// Stop the runtime and cleanly free all associated memory and file descriptors.
//
// Performs a multi-step shutdown sequence to ensure graceful degradation.
// First, it halts the Thread_Group, stopping execution.
// Next, it shuts down all reactors, which closes underlying `epfd` or `kq_fd` descriptors.
// Finally, it traverses the `active_fibers` linked list to free any suspended-forever fibers,
// preventing memory leaks for tasks that never resumed.
feather_destroy :: (flush_time := 5000) {
    runtime := context.feather;
    ok := shutdown(*runtime.exec.group, 5000);
    if !ok {
        print("\n\n[WARNING] Thread_Group shutdown timed out! Threads are stuck and leaking!\n\n");
    }

    for i: 0..runtime.reactors.count-1 {
        shutdown(*runtime.reactors[i]);
    }

    // Clean up leaked fibers (ones suspended infinitely)
    // First, steal the list
    lock(*runtime.active_fibers_mutex);
    curr := runtime.active_fibers_head;
    runtime.active_fibers_head = null;
    unlock(*runtime.active_fibers_mutex);

    while curr {
        next := curr.next_active;
        // Don't set feather to null, fiber_destroy needs it for stack_pool.
        // It will safely do nothing to the list because we set head to null.
        curr.prev_active = null;
        curr.next_active = null;

        owner := curr.owner_future;
        fiber_destroy(curr);
        if owner free(owner);
        curr = next;
    }

    destroy(*runtime.active_fibers_mutex);

    deinit_stack_pool(*runtime.stack_pool);
    array_free(runtime.timers);

    array_free(runtime.reactors);
    array_free(runtime.reactor_threads);
    free(runtime); context.feather = null;
}

#import "System";
#import "Thread";
#import "Atomics";



// Helper for feather_block_on
#scope_file
Block_On_Args :: struct {
    entry_fn: () -> ();
    pending_fibers: *s64;
}

block_on_helper :: () {
    args := cast(*Block_On_Args) context.workspace;
    args.entry_fn();
    atomic_add(args.pending_fibers, -1);
}
#scope_export

// Execute an asynchronous closure ergonomically from a synchronous context.
//
// Provides a single-call wrapper around
// `feather_main`, `feather_spawn`, `feather_run_until_done`, and `feather_destroy`.
// It bundles arguments via `Block_On_Args` to inject the main task.
// `block_on_helper` acts as the root fiber, decrementing the `pending_fibers`
// atomic counter upon completion to signal the runtime to tear itself down.
feather_block_on :: (entry_fn: () -> (), thread_count: s32 = -1) {
    created_runtime := false;
    if !context.feather {
        feather_main(thread_count);
        created_runtime = true;
    }

    pending_fibers : s64 = 1; // start with 1 for the root fiber

    args: Block_On_Args;
    args.entry_fn = entry_fn;
    args.pending_fibers = *pending_fibers;

    // Spawn root fiber
    new_ctx := context;
    new_ctx.workspace = cast(*void) *args;
    push_context new_ctx {
        fiber_spawn_injected(block_on_helper);
    }

    feather_run_until_done(*pending_fibers, 9999999);

    if created_runtime {
        feather_destroy();
    }
}

// Spawn a detached fiber and immediately schedule it for background execution.
//
// Provides a fire-and-forget convenience over `fiber_spawn_injected`.
feather_spawn :: (fn: () -> (), stack_size: s64 = 524288) {
    fiber_spawn_injected(fn, stack_size);
}
