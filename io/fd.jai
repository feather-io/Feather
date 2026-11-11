#import "Basic";
#import "Socket";

// Wait until fd is readable. Suspends the current fiber.
feather_wait_readable :: (fd: s32) {
    yield_injected(cast(Socket) fd, .READ);
    fiber_yield();
}

// Wait until fd is writable. Suspends the current fiber.
feather_wait_writable :: (fd: s32) {
    yield_injected(cast(Socket) fd, .WRITE);
    fiber_yield();
}
