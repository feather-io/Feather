# 🪶 Feather

A hybrid asynchronous runtime module for the Jai programming language.

> ⚠️ Before using it for any critical systems, be warned that the techniques used here are highly experimental and opinionated.

Feather let's you write both synchronous looking non-blocking code using **stackful Fibers** and high throughput tasks as zero allocation **stackless Futures** on the exact same executor.


### Why Feather?

I have been programming for more than 15 years and I find systems programming language choosing either one of the above solutions to build their ecosystem on top of it.

1. **Stackless (Rust, C#, JavaScript):** Zero-cost state machines, but you get the **"function coloring problem"** (`async` infects your whole codebase). Code is split into two incompatible worlds.

2. **Stackful (Go, Erlang):** Write standard blocking code, but **every single task carries a call stack**. 100,000 idle connections will swallow gigabytes of RAM.

This runtime is an experiment on how we can run both Stackless and Stackful routines on a same executor. You can take a look at the architecture below on how it actually does that.

> To explore if this runtime works on any meaningful real world use cases I created [Falcon](https://github.com/feather-io/Falcon). This is a high volume HTTP module for Jai built on top of Feather runtime and it's primitives.

### Architecture

```
                          +---------------------------------------+
                          |               Executor                |
                          |  (Thread_Group Work-Stealing Workers) |
                          +---------------------------------------+
                                        ^           |
                                 wake() |           | fut.poll()
                                        |           v
    +------------------+         +----------------------------+
    |  OS Kernel Event |  kevent |          Reactor           |
    | (epoll / kqueue) | <-----> |   (One per CPU core loop)  |
    +------------------+         +----------------------------+
                                        |
                                (EWOULDBLOCK)
                                        v
                                 +--------------+
                                 | fiber_yield  | ---> fiber_swap() assembly
                                 +--------------+
```

### Ideas _(Why I call it experimantal..)_

This runtime is full of ideas that I thought would be a right choice for a systems programming language like Jai where freedom to implement makes more sense than constraints.

1. Transparent Asynchrony: Network functions like `stream_read` and `stream_write` check for `context.feather`. If called inside a fiber, they yield cooperatively on `EWOULDBLOCK`. If on standard blocking code it acts in a blocking way calling POSIX I/O.
2. 4 State Lock-Free State Machine: In order to maintain the `scheduled_state` of each work item it uses CAS state machine. This is the second iteration of the runtime over a Mutex.

```
IDLE ──(wake)──> QUEUED ──(worker picks up)──> RUNNING
                                                     │
              wake() called while RUNNING            │
              RUNNING ──(CAS)──> RUNNING_AND_WOKEN   │
                                        │            │
              worker finishes poll      │            │
              RUNNING_AND_WOKEN ──(CAS)──> QUEUED ──(spawn)
```

3. Zero-Cost Polymorphism: `Fiber_Future` (meaning a stackful Fiber is also internally a Future which has a stack) uses  `#as using base: Future` at byte offset 0. The executor drives both fibers and futures through a single pointer without vtables or dynamic dispatch.

## Current Limitations & Roadmap

Feather is currently in alpha. I feel that Feather needs Falcon and Falcon needs Feather and Falcon needs production usage to grow 😉 . While the core scheduling and I/O mechanics are stable, several areas are actively being improved:

- [ ] Unification of Approaches - Since I'm experimenting with stuff I would also like to experiment with **Unified Notification Mechanism (UNM)** which I think would implementing two different control flows for Rediness and Completion based models.
- [ ] Cancellation - This is one of the main fundamental building block of asynchronous / distributed system which is pending to be implemented. I think that UNM still holds priority, and cancellation will be easy after that.
- [ ] Chase-Lev lock-free deque per worker thread - Not sure on % the impact over the current work stealing 

 ## Platform Support

Operating System | Architecture    | Backend      | Status
-----------------|-----------------|--------------|-------------------------------------
macOS            | ARM64 (aarch64) | kqueue       | Tier 1 (Primary dev platform)
Linux            | x86_64 / arm64  | epoll        | Supported (Validated in containers)
Windows          | ❌              | IOCP         | ❌

## License

MIT License. Feel free to use, hack on, and embed Feather in your Jai projects.

Developed on `Version: beta 0.2.030, built on 2 July 2026.`
