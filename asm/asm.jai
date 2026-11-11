#scope_module

// Link the platform-specific assembly object.
// You must assemble this first:
//   as -o fiber_swap_arm64.o fiber_swap_arm64.s
//   ar rcs fiber_swap_arm64.a fiber_swap_arm64.o
#if OS == .MACOS || (OS == .LINUX && CPU == .ARM64) {
    OS_Register :: Register_Mac_ARM64;
} else #if OS == .LINUX && CPU == .X64 {
    OS_Register :: Register_X64;
}
#if OS == .MACOS {
    fiber_asm :: #library,no_dll "fiber_swap_arm64";
} else #if OS == .LINUX && CPU == .ARM64 {
    fiber_asm :: #library,no_dll "fiber_swap_linux_arm64";
} else #if OS == .LINUX && CPU == .X64 {
    fiber_asm :: #library,no_dll "fiber_swap_linux_x64";
}

// The context-switch primitive - implemented in raw assembly because
// we cannot manipulate SP/PC from a high-level language.
fiber_swap :: (old_ctx: *Fiber_Context, new_ctx: *Fiber_Context) #foreign fiber_asm;
