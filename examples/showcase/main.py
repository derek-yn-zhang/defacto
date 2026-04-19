"""Showcase: multi-entity, multi-source, identity resolution, merges, erasure.

    pip install defacto
    python examples/showcase/main.py
"""

from defacto import Defacto


def main():
    d = Defacto("examples/showcase/")

    # App events: signups, subscriptions, upgrades, logins
    d.ingest("app", [
        {"type": "signup", "timestamp": "2024-01-01T10:00:00Z",
         "email": "alice@example.com", "phone": "+1-555-0001"},
        {"type": "subscribe", "timestamp": "2024-01-02T09:00:00Z",
         "email": "alice@example.com", "plan": "pro", "mrr": 49},
        {"type": "signup", "timestamp": "2024-01-05T14:00:00Z",
         "email": "bob@example.com", "phone": "+1-555-0002"},
        {"type": "subscribe", "timestamp": "2024-01-06T11:00:00Z",
         "email": "bob@example.com", "plan": "starter", "mrr": 19},
        {"type": "upgrade", "timestamp": "2024-02-01T10:00:00Z",
         "email": "alice@example.com", "plan": "enterprise", "mrr": 199},
        {"type": "login", "timestamp": "2024-02-15T08:00:00Z",
         "email": "alice@example.com"},
        {"type": "login", "timestamp": "2024-02-20T09:00:00Z",
         "email": "alice@example.com"},
    ], process=True)

    # Billing events: orders with different field names, linked via email
    d.ingest("billing", [
        {"event": "order.created", "created_at": "2024-01-10T16:00:00Z",
         "id": "ORD-001", "customer_email": "alice@example.com",
         "amount": 299.99, "line_item_count": 3},
        {"event": "order.created", "created_at": "2024-01-20T12:00:00Z",
         "id": "ORD-002", "customer_email": "bob@example.com",
         "amount": 49.99, "line_item_count": 1},
        {"event": "order.shipped", "created_at": "2024-01-12T10:00:00Z",
         "id": "ORD-001"},
        {"event": "order.delivered", "created_at": "2024-01-15T14:00:00Z",
         "id": "ORD-001"},
    ], process=True)

    # Current state
    print("Customers:")
    df = d.table("customer").execute()
    print(df[["customer_state", "email", "plan", "mrr",
              "annual_revenue", "login_count"]].to_string(index=False))

    print("\nOrders:")
    df = d.table("order").execute()
    print(df[["order_state", "order_id", "total", "item_count"]].to_string(index=False))

    # Point-in-time: what did we know on January 3rd?
    print("\nCustomers on January 3rd:")
    df = d.history("customer").as_of("2024-01-03").execute()
    print(df[["customer_state", "email", "plan", "mrr"]].to_string(index=False))

    # Full history for alice
    print("\nAlice's history:")
    alice_id = d._identity_backend.lookup("customer", "alice@example.com")
    df = d.history("customer").execute()
    df = df[df["customer_id"] == alice_id]
    print(df[["customer_state", "plan", "mrr", "annual_revenue",
              "valid_from", "valid_to"]].to_string(index=False))

    # Merge: carol signs up with alice's phone number
    d.ingest("app", [
        {"type": "signup", "timestamp": "2024-03-01T10:00:00Z",
         "email": "carol@work.com", "phone": "+1-555-0001"},
    ], process=True)

    print("\nCustomers after merge:")
    df = d.table("customer").execute()
    print(df[["customer_state", "email", "plan", "mrr"]].to_string(index=False))

    # Both emails resolve to the same entity
    alice_id = d._identity_backend.lookup("customer", "alice@example.com")
    carol_id = d._identity_backend.lookup("customer", "carol@work.com")
    print(f"\nalice@example.com -> {alice_id[:8]}...")
    print(f"carol@work.com    -> {carol_id[:8]}...")

    # Timeline
    print(f"\nAlice's timeline:")
    timeline = d.timeline(alice_id)
    for entry in timeline.entries:
        state = entry.state_after or "?"
        print(f"  {entry.timestamp}  {state:>10}  {entry.effects}")

    # Erase bob (GDPR)
    bob_id = d._identity_backend.lookup("customer", "bob@example.com")
    if bob_id:
        result = d.erase(bob_id)
        print(f"\nErased bob: {result.events_deleted} events, "
              f"{result.entities_erased} entities")

    print("\nCustomers after erasure:")
    df = d.table("customer").execute()
    print(df[["customer_state", "email", "plan"]].to_string(index=False))
    print(f"bob lookup: {d._identity_backend.lookup('customer', 'bob@example.com')}")

    d.close()


if __name__ == "__main__":
    main()
