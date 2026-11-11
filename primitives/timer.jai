#import "Basic";
#import "Thread";

// Suspend the current fiber for at least `ms` milliseconds.
//
// This is a non-blocking sleep. Unlike `sleep_milliseconds()`, this procedure does
// NOT stall the OS thread. It registers the current fiber's Future into the runtime's
// centralized timer list, then immediately yields the fiber back to the scheduler.
//
// The Reactor's `poll_reactor()` checks the timer list on every iteration. When 
// `seconds_since_init() * 1000 >= wake_time_ms`, it calls `wake()` on the stored 
// Future, which re-queues the fiber on the Executor.
feather_sleep_ms :: (ms: s64) {
    if !context.feather {
        assert(false, "feather_sleep_ms called outside of a Feather runtime");
        return;
    }
    
    if ms <= 0 return;
    
    fut := context.waker.fut;
    wake_time := (seconds_since_init() * 1000.0) + cast(float64) ms;
    
    entry: Timer_Entry;
    entry.wake_time_ms = wake_time;
    entry.fut = fut;
    
    // The timers list is shared across all reactor threads, so it requires a mutex.
    lock(*context.feather.timers_mutex);
    array_add(*context.feather.timers, entry);
    unlock(*context.feather.timers_mutex);
    
    // Yield without registering any IO event. The timer list will re-wake this fiber.
    fiber_yield();
}
