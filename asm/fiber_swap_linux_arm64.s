// fiber_swap(old_ctx: *Fiber_Context, new_ctx: *Fiber_Context)
//
// Linux ARM64 (AArch64) calling convention:
//   x0 = first argument  (old_ctx pointer)
//   x1 = second argument (new_ctx pointer)
//   x30 = link register  (return address)
//

.global fiber_swap
.type fiber_swap, %function
.align 4

fiber_swap:
    // ===== SAVE old context (x0 points to old_ctx) =====

    // Save sp
    mov     x2, sp
    str     x2,  [x0, #0]

    // Save pc (link register = return address of the caller)
    str     x30, [x0, #8]

    // Save fp (frame pointer)
    str     x29, [x0, #16]

    // Save callee-saved general purpose registers x19-x28
    stp     x19, x20, [x0, #24]
    stp     x21, x22, [x0, #40]
    stp     x23, x24, [x0, #56]
    stp     x25, x26, [x0, #72]
    stp     x27, x28, [x0, #88]

    // Save callee-saved SIMD/float registers d8-d15
    stp     d8,  d9,  [x0, #104]
    stp     d10, d11, [x0, #120]
    stp     d12, d13, [x0, #136]
    stp     d14, d15, [x0, #152]

    // ===== LOAD new context (x1 points to new_ctx) =====

    // Restore callee-saved SIMD/float registers d8-d15
    ldp     d8,  d9,  [x1, #104]
    ldp     d10, d11, [x1, #120]
    ldp     d12, d13, [x1, #136]
    ldp     d14, d15, [x1, #152]

    // Restore callee-saved general purpose registers x19-x28
    ldp     x19, x20, [x1, #24]
    ldp     x21, x22, [x1, #40]
    ldp     x23, x24, [x1, #56]
    ldp     x25, x26, [x1, #72]
    ldp     x27, x28, [x1, #88]

    // Restore fp
    ldr     x29, [x1, #16]

    // Restore pc into x30 (link register)
    ldr     x30, [x1, #8]

    // Restore sp
    ldr     x2,  [x1, #0]
    mov     sp,  x2

    // Jump to the restored return address
    ret
