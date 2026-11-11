// Feather Fiber - Stackful Coroutines
//
// A Fiber is a user-space thread with its own stack. Unlike Futures
// (stackless, zero-cost state machines), Fibers let the developer
// write completely synchronous blocking code. The fiber_yield()
// call saves the entire CPU register state and switches back to
// the scheduler - no state machines, no AWAIT.
//
// Tradeoff: Each Fiber allocates a private stack on the heap
// (default 64KB). 10,000 Fibers = ~640MB. Use Futures for
// lightweight high-concurrency workloads, Fibers for developer
// ergonomics.

#import "Basic";
#load "asm/asm.jai";

// ---------------------------------------------------------------
// Scheduler state
// ---------------------------------------------------------------
#add_context scheduler_context: *Fiber_Context;
#add_context current_fiber: *Fiber;

// The fundamental unit of stackful execution in Feather.
//
// A Fiber allocates its own private stack and saves its entire CPU
// register state during a context switch. Because it maintains its own
// call stack, it allows blocking operations to yield cooperatively back
// to the asynchronous executor without requiring the user to write state
// machines.
Fiber :: struct {
    // The saved CPU register state of the fiber.
    //
    // This MUST be the first field in the struct. Because `context_buf`
    // is at offset 0, a pointer to a `Fiber` is numerically equal to a
    // pointer to its `Fiber_Context`. This enables the assembly
    // trampoline to safely cast `*Fiber_Context` back into `*Fiber`.
    context_buf:  Fiber_Context;

    // The private, heap-allocated execution stack for this fiber.
    stack_memory: *void;

    // The size of the allocated stack in bytes.
    stack_size:   s64;

    // The user-provided entry function that this fiber will execute.
    entry_fn:     () -> ();

    // True if the fiber has completely finished executing its entry function.
    finished:     bool;

    // The Jai context captured at fiber creation.
    //
    // This allows the fiber to inherit the environment it was spawned in.
    // More importantly, it enables multi-threaded fiber migration: if a fiber
    // yields and is later resumed on a DIFFERENT worker thread, this context
    // ensures it correctly synchronizes with the new thread's waker and scheduler.
    jai_context:  type_of(context);

    // The runtime environment that owns this fiber.
    //
    // Captured separately from `jai_context` because by the time the fiber
    // finishes and is destroyed, the original thread context may no longer
    // be accessible or valid. This is necessary for unlinking from the global list.
    feather: *Feather_Runtime;

    // Pointer to the next active fiber in the global linked list.
    next_active: *Fiber;

    // Pointer to the previous active fiber in the global linked list.
    //
    // This doubly-linked list allows the global runtime to track all active
    // fibers so it can gracefully clean up any indefinitely suspended fibers
    // during teardown.
    prev_active: *Fiber;

    // Pointer to the Future that bridges this fiber to the executor.
    owner_future: *void; // Points to Fiber_Future
}

Fiber_Context :: struct {
    using register: OS_Register;
}

// ---------------------------------------------------------------
// The trampoline - this is the first code that runs on the fiber's
// private stack. fiber_swap's `ret` jumps here because we set
// context_buf.pc to its address during fiber_create.
//
// Because fiber_swap doesn't clobber x0 and x1, they still contain
// old_ctx and new_ctx respectively. We can receive them as arguments!
// ---------------------------------------------------------------

// The assembly-level entry point for a newly started fiber.
//
// When `fiber_create` sets up a new fiber, it points the instruction pointer
// (`pc` on ARM64, return address pushed on x86_64) to this function. When the
// very first `fiber_swap` returns via its `ret` instruction, it jumps here
// instead of returning to a previous caller.
//
// Because `fiber_swap` does not clobber the first two argument registers (`x0`
// and `x1` on ARM64, `rdi` and `rsi` on x86_64), they still contain the pointers
// to the `sched_ctx` and `fiber_ctx` that were passed into `fiber_swap`. Thus,
// this trampoline receives them directly as arguments.
//
// Note that at the end of the entry function, the fiber must return to the
// scheduler. However, it must use the *latest* `scheduler_context` from the
// fiber's `jai_context`, because the fiber may have yielded and been resumed
// many times. The `sched_ctx` parameter passed into this function is from the
// *first* resume and is therefore stale by the time the fiber finishes.
fiber_trampoline :: (sched_ctx: *Fiber_Context, fiber_ctx: *Fiber_Context) #c_call {
    // context_buf is the first member of Fiber, so their pointers are identical.
    fiber := cast(*Fiber) fiber_ctx;

    push_context fiber.jai_context {
        context.scheduler_context = sched_ctx;
        context.current_fiber = fiber;

        fiber.entry_fn();

        fiber.finished = true;

        // Just like fiber_yield, when the fiber completely finishes, we
        // MUST jump back to the LATEST scheduler_context! The 'sched_ctx' parameter
        // passed into this trampoline is from the very first resume, and is now stale garbage.
        fiber_swap(fiber_ctx, fiber.jai_context.scheduler_context);
    }
}

// ---------------------------------------------------------------

// Allocate a new fiber and initialize its CPU register context.
//
// This function sets up a private stack and crafts the initial register
// state so that a context switch (`fiber_swap`) will seamlessly begin
// execution at `fiber_trampoline`.
//
// The stack setup differs based on the architecture:
//
// 1. **ARM64**: The stack pointer (`sp`) is set to the very top of the allocated
//    memory. The program counter (`pc`) is explicitly set to the address of
//    `fiber_trampoline`. When `fiber_swap` executes its final `ret` instruction,
//    it branches directly to `pc`.
//
// 2. **x86_64**: The address of `fiber_trampoline` is pushed onto the very top
//    of the newly allocated stack, and the stack pointer (`sp`) is set to point
//    to it. When `fiber_swap` executes `ret`, it pops this address off the stack
//    and jumps to it.
//
// On both architectures, the stack must be aligned to 16 bytes before the
// call into `fiber_trampoline` begins, which is a strict ABI requirement for
// vector instructions and C interoperability.
fiber_create :: (entry_fn: () -> (), stack_size: s64 = 524288) -> *Fiber {
    fiber := New(Fiber);
    fiber.entry_fn    = entry_fn;
    fiber.stack_size  = stack_size;
    fiber.finished    = false;

    // Capture the current Jai context so the fiber inherits it.
    fiber.jai_context = context; fiber.jai_context.stack_trace = null;
    fiber.feather = context.feather;

    // Acquire a stack from the pool (or allocate a fresh one if sizes don't match).
    if stack_size == context.feather.stack_pool.stack_size {
        fiber.stack_memory = pool_acquire(*context.feather.stack_pool);
    } else {
        fiber.stack_memory = alloc(stack_size);
    }

    // ARM64 stacks grow downward. SP must start at the TOP of the
    // allocated block, aligned to 16 bytes (ARM64 ABI requirement).
    stack_top : u64 = cast(u64) fiber.stack_memory + cast(u64) stack_size;
    stack_top &= ~cast(u64) 0xF; // 16-byte align

    // Set up the initial register context so that the first
    // fiber_swap into this fiber lands on the trampoline.
    #if CPU == .ARM64 {
        fiber.context_buf.sp = stack_top;
        fiber.context_buf.pc = cast(u64) cast(*void) fiber_trampoline;
        // fp starts at the same place as sp (empty call stack).
        fiber.context_buf.fp = 0;
    } else #if CPU == .X64 {
        // In x86_64, the return address is popped from the stack by `ret`.
        // We push fiber_trampoline onto the new stack.
        stack_top -= 8;
        (cast(*u64) stack_top).* = cast(u64) cast(*void) fiber_trampoline;

        // Ensure 16-byte alignment before the call (the `ret` instruction
        // effectively acts as a call return, so the stack should be 16-byte
        // aligned at the entry of fiber_trampoline - which it is, because
        // we subtracted 8 from a 16-byte aligned address).
        fiber.context_buf.sp = stack_top;
        fiber.context_buf.bp = stack_top;
    }

    return fiber;
}

// Switch execution from the current thread (scheduler) to the fiber.
//
// This allocates a temporary `Fiber_Context` on the current thread's stack
// to act as the `sched_ctx`. It sets the context pointers to point to this
// fiber, and then performs the actual CPU register context switch using
// `fiber_swap`. Execution returns from this function only when the fiber
// explicitly yields or finishes.
fiber_resume :: (fiber: *Fiber) {
    sched_ctx: Fiber_Context;

    context.current_fiber = fiber;
    context.scheduler_context = *sched_ctx;

    // Also pass to fiber.jai_context so fiber_yield sees them when running inside the fiber
    fiber.jai_context.current_fiber = fiber;
    fiber.jai_context.scheduler_context = *sched_ctx;
    fiber.jai_context.waker = context.waker;
    fiber.jai_context.feather = context.feather;

    fiber_swap(*sched_ctx, *fiber.context_buf);
}

// Suspend the current fiber and return control to the scheduler.
//
// This is the core cooperative yield mechanism. It saves the fiber's CPU
// registers and swaps back to the scheduler's context.
//
// A critical architectural insight here is that it reads `scheduler_context`
// from the HEAP-allocated `Fiber` object (`fiber.jai_context.scheduler_context`),
// rather than relying solely on the local stack `context`. When the fiber wakes up,
// it must synchronize its local `waker` and `feather` pointers from the heap because
// a work-stealing executor might have resumed this fiber on an entirely DIFFERENT
// worker thread. If we don't sync, the fiber would continue using stale pointers
// from the old thread.
fiber_yield :: () {
    // We must read the scheduler_context from the heap-allocated Fiber object!
    fiber := context.current_fiber;
    fiber_swap(*fiber.context_buf, fiber.jai_context.scheduler_context);

    // ✨ WHEN WE WAKE UP, update our local fiber context!
    // The fiber_resume function updated fiber.jai_context on the heap with the NEW worker thread's
    // variables (like 'waker'). If we don't copy them into our local stack context, we will use
    // stale pointers from the previous thread!
    context.scheduler_context = fiber.jai_context.scheduler_context;
    context.waker = fiber.jai_context.waker;
    context.feather = fiber.jai_context.feather;
}

// Yield execution cooperatively without waiting for I/O.
//
// Unlike `fiber_yield`, which implies the fiber is waiting for an external
// event (like a socket becoming readable) to wake it up, this function
// immediately re-queues the fiber on the executor by calling `wake()`
// before swapping back to the scheduler.
//
// This allows compute-heavy fibers to yield periodically so they don't
// starve other asynchronous tasks on the same worker thread.
fiber_yield_and_reschedule :: () {
    fiber := context.current_fiber;
    waker  := context.waker;

    // Immediately re-queue ourselves BEFORE swapping back.
    // This means the executor will pick us up again on the next poll cycle.
    wake(waker.fut);

    // Now swap back to the scheduler.
    fiber_swap(*fiber.context_buf, fiber.jai_context.scheduler_context);

    // Sync context pointers when we resume (same as fiber_yield).
    context.scheduler_context = fiber.jai_context.scheduler_context;
    context.waker = fiber.jai_context.waker;
    context.feather = fiber.jai_context.feather;
}

// Deallocate the fiber and its resources.
//
// This first safely unlinks the fiber from the global `active_fibers`
// doubly-linked list. It then returns the fiber's stack memory back to
// the thread-safe `Fiber_Stack_Pool`. If the stack was allocated with
// a non-standard size (not matching the pool's size), it is freed directly
// back to the operating system.
fiber_destroy :: (fiber: *Fiber) {
    if fiber.feather {
        lock(*fiber.feather.active_fibers_mutex);
        if fiber.prev_active {
            fiber.prev_active.next_active = fiber.next_active;
        } else {
            if fiber.feather.active_fibers_head == fiber {
                fiber.feather.active_fibers_head = fiber.next_active;
            }
        }
        if fiber.next_active {
            fiber.next_active.prev_active = fiber.prev_active;
        }
        unlock(*fiber.feather.active_fibers_mutex);
    }

    if fiber.stack_memory {
        if fiber.stack_size == fiber.feather.stack_pool.stack_size {
            pool_release(*fiber.feather.stack_pool, fiber.stack_memory);
        } else {
            free(fiber.stack_memory);
        }
    }
    free(fiber);
}

// ===============================================================
// Fiber Stack Pool reuses 64KB stack allocations instead of
// calling alloc/free on every connection.
// ===============================================================

// A thread-safe free-list of pre-allocated fiber stacks.
//
// Allocating and freeing a 64KB block on every HTTP connection or fiber
// spawn would introduce unacceptable latency and memory fragmentation.
// This pool mitigates that by caching freed stacks. Note that the pool
// only caches stacks of a specific size (the default `stack_size`).
// Fibers requiring larger or smaller stacks bypass this pool and incur
// standard allocation costs.
Fiber_Stack_Pool :: struct {
    mutex: Mutex;
    free_stacks: [..] *void;
    stack_size: s64;
}

init_stack_pool :: (pool: *Fiber_Stack_Pool, stack_size: s64 = 524288) {
    init(*pool.mutex);
    pool.stack_size = stack_size;
}

pool_acquire :: (pool: *Fiber_Stack_Pool) -> *void {
    lock(*pool.mutex);
    defer unlock(*pool.mutex);
    if pool.free_stacks.count > 0 {
        return pop(*pool.free_stacks);
    }
    return alloc(pool.stack_size);
}

pool_release :: (pool: *Fiber_Stack_Pool, mem: *void) {
    lock(*pool.mutex);
    defer unlock(*pool.mutex);
    array_add(*pool.free_stacks, mem);
}

deinit_stack_pool :: (pool: *Fiber_Stack_Pool) {
    for pool.free_stacks  free(it);
    array_free(pool.free_stacks);
}

// ===============================================================
// Fiber_Future - The bridge between Fibers and the Executor.
//
// The Executor only understands *Future. This struct wraps a *Fiber
// so that worker_proc can schedule it like any other Future.
// The Executor, Reactor, and Waker remain completely untouched.
// ===============================================================

// The bridge bridging the stackful Fiber world with the zero-cost Future/Executor world.
//
// The asynchronous executor only understands `Future` objects. By embedding
// `#as using base: Future`, `Fiber_Future` becomes seamlessly castable to a
// `*Future`. This allows a heavy, stackful coroutine to be scheduled on the
// exact same executor that runs lightweight, state-machine futures.
Fiber_Future :: struct {
    #as using base: Future;
    fiber: *Fiber;
}

// Evaluate the progress of the underlying fiber.
//
// When the executor polls this future, it translates the poll into a
// `fiber_resume` call, physically switching the CPU context to the fiber's
// private stack. If the fiber finishes its execution, this function cleans
// up the fiber by calling `fiber_destroy` and returns `READY`. If the fiber
// yields (e.g., waiting on I/O), it returns `PENDING`, leaving the future
// asleep until the reactor wakes it up.
fiber_future_poll :: (base: *Future) -> Poll_State {
    self := cast(*Fiber_Future) base;

    // If a previous resume already finished the fiber (shouldn't
    // normally happen, but guard against double-poll).
    if self.fiber.finished {
        fiber_destroy(self.fiber);
        self.fiber = null;
        return .READY;
    }

    // Pass the executor's waker to the fiber's context so that when it yields,
    // the reactor knows how to wake this Fiber_Future back up.
    self.fiber.jai_context.waker = context.waker;

    // Jump onto the fiber's private stack and run its code.
    // This returns when the fiber calls fiber_yield() or its
    // entry_fn returns (trampoline sets finished = true).
    fiber_resume(self.fiber);

    if self.fiber.finished {
        fiber_destroy(self.fiber);
        self.fiber = null;
        return .READY;
    }

    // The fiber yielded - it registered an FD with the Reactor
    // before yielding, so the Reactor will wake() our Waker
    // which re-spawns this Fiber_Future on the Executor.
    return .PENDING;
}

// Allocate a fiber and schedule it on the executor.
//
// This is a high-level wrapper that both allocates the heap resources for
// the `Fiber` and its associated `Fiber_Future` bridge, links the new fiber
// into the global doubly-linked active list for safe teardown, and finally
// spawns it onto the provided asynchronous `Executor`.
fiber_spawn :: (exec: *Executor, entry_fn: () -> (), stack_size: s64 = 524288) {
    fiber := fiber_create(entry_fn, stack_size);

    fut := New(Fiber_Future);
    fut.poll  = fiber_future_poll;
    fut.fiber = fiber;
    fiber.owner_future = fut;

    // Link to active fibers list
    if context.feather {
        lock(*context.feather.active_fibers_mutex);
        fiber.next_active = context.feather.active_fibers_head;
        if context.feather.active_fibers_head  context.feather.active_fibers_head.prev_active = fiber;
        context.feather.active_fibers_head = fiber;
        unlock(*context.feather.active_fibers_mutex);
    }

    spawn(exec, fut);
}

fiber_spawn_injected :: (entry_fn: () -> (), stack_size: s64 = 524288) {
    fiber_spawn(*context.feather.exec, entry_fn, stack_size);
}
