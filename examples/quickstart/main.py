"""Quickstart: define entities, ingest events, query state.

    pip install defacto
    python examples/quickstart/main.py
"""

from defacto import Defacto

d = Defacto("examples/quickstart/")

# Ingest some events
d.ingest("app", [
    {"type": "signup", "timestamp": "2024-01-01T10:00:00Z", "email": "alice@example.com"},
    {"type": "signup", "timestamp": "2024-01-05T14:00:00Z", "email": "bob@example.com"},
    {"type": "upgrade", "timestamp": "2024-01-15T09:00:00Z", "email": "alice@example.com", "plan": "pro"},
    {"type": "cancel", "timestamp": "2024-03-01T12:00:00Z", "email": "bob@example.com"},
], process=True)

# Current state
print("Current state:")
df = d.table("customer").execute()
print(df[["customer_id", "customer_state", "email", "plan"]].to_string(index=False))

# What did things look like on January 10th?
print("\nState on January 10th:")
df = d.history("customer").as_of("2024-01-10").execute()
print(df[["customer_state", "email", "plan"]].to_string(index=False))

# Full history
print("\nFull history:")
df = d.history("customer").execute()
print(df[["customer_state", "email", "plan", "valid_from", "valid_to"]].to_string(index=False))

d.close()
