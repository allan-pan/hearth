//! driver, operators, expression evaluation

#[cfg_attr(
    not(test),
    expect(dead_code, reason = "execution operators not wired yet")
)]
mod eval;
