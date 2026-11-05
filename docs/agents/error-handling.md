# Error Handling Guidelines

Read this before adding or changing error creation, wrapping, or propagation.

- Use predefined errors from the repository error package; keep the local import name consistent with surrounding code.
- When an error comes from a third-party or library call, wrap it immediately at the boundary with `errors.WrapError(predefinedError, err, args...)`.
- For new or changed code, do not use `errors.Trace` as the initial wrapper for third-party or library errors. If no predefined error fits, choose an existing predefined error or add an appropriate one, then use `errors.WrapError`.
- After an error has been wrapped with `errors.WrapError`, propagate it directly; do not call `errors.Trace` on later paths.
- When creating a TiCDC error, use `GenWithStack...` or `GenWithStackByArgs...` on a predefined error and pass concrete details through arguments when needed.
- Decide whether a newly generated error needs stack information. If a stack is unnecessary, especially on hot paths, use `FastGen...` or `FastGenByArgs...`.
- Avoid other error creation or wrapping styles in new or changed code, including `errors.New`, bare `fmt.Errorf`, and adding stack information multiple times.
