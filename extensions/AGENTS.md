# Backend extension guidance

Each child directory is a package tree in the root module (store, MCP, HTTP,
browser, and search backends). Provider adapters live in `providers/` and
follow the same rules.

- Translate vendor requests, responses, streaming deltas, usage, errors, tool
  schemas, and stop reasons at this boundary.
- Preserve every provider-neutral block the vendor supports. Use `RawBlock` for
  provider-specific content that must round-trip.
- Never import vendor dependencies from `core`, `agents`, `tools`, `retry`, or
  `tracing`; keep them inside the extension package that needs them.
- Wrap transient vendor failures with a type implementing `retry.Retryable`;
  preserve context cancellation unchanged.
- Add conversion tests for each supported block and malformed-input path.
- Keep streaming and non-streaming response semantics aligned.
- When `core` changes, review every extension even if only one adapter needs a
  code edit.

Use `module_integrator` for cross-provider impact analysis. Test an affected
package with `go test ./extensions/<name>/...` from the repository root.
