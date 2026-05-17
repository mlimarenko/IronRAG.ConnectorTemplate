"""Echo connector — a trivial reference implementation.

Shows the minimum a SourceAdapter must do: enumerate items with a stable
change_token, fetch their bytes, and round-trip an external_key through
``external_key`` / ``parse_external_key``. The "source" is a dict in
memory; swap in your vendor's REST calls to turn it into a real connector.
"""
