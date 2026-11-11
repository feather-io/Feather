.global fiber_swap
.type fiber_swap, @function

// fiber_swap(old_ctx: *Fiber_Context, new_ctx: *Fiber_Context)
// rdi = old_ctx
// rsi = new_ctx

fiber_swap:
    // Save current state to old_ctx (rdi)
    movq %rsp, 0(%rdi)
    movq %rbp, 8(%rdi)
    movq %rbx, 16(%rdi)
    movq %r12, 24(%rdi)
    movq %r13, 32(%rdi)
    movq %r14, 40(%rdi)
    movq %r15, 48(%rdi)

    // Load new state from new_ctx (rsi)
    movq 0(%rsi), %rsp
    movq 8(%rsi), %rbp
    movq 16(%rsi), %rbx
    movq 24(%rsi), %r12
    movq 32(%rsi), %r13
    movq 40(%rsi), %r14
    movq 48(%rsi), %r15

    // Return to the new context's saved return address
    ret
