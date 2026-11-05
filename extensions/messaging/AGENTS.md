# Messaging guidance

- Keep `messaging` and its channel subpackages standard library only.
- Channels must verify the platform's signature or secret before building an
  `Inbound`, and must never let model text choose button labels or actions.
- Approving must always take an explicit button press from a listed user;
  free text may only deny or answer a question.
- Router records are written before the post or send they describe, so
  restarts repost at most once. Keep handling of redelivered events idempotent.
- Test channels against an `httptest` fake of the platform API; never require
  a live token.
