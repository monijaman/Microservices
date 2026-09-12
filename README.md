# Microservices

A practical local lab for learning microservices, Kafka, event-driven architecture, and Kubernetes.

This workspace contains:

- a full microservices practice guide in [MICROSERVICES_PRACTICE.md](MICROSERVICES_PRACTICE.md)
- a working, beginner-friendly, heavily-commented implementation of the guide's "First Milestone" (Order + Inventory + Payment services talking over Kafka, with Postgres and the Saga pattern)
- a Kubernetes-focused practice path that starts after the Docker Compose version is working

## Quick start

```bash
docker compose up --build
```

This starts Kafka, Kafka UI, Postgres, Redis, and the three Go microservices.

Wait ~15-20 seconds for everything to become healthy, then create an order:

```bash
curl -X POST http://localhost:8081/orders \
  -H "Content-Type: application/json" \
  -d '{"item": "widget", "quantity": 2}'
```

Copy the `id` from the response and check its status a moment later:

```bash
curl http://localhost:8081/orders/<id>
```

Watch it happen live:

- `docker compose logs -f order-service inventory-service payment-service` — see each service react to the Kafka events
- http://localhost:8080 — Kafka UI, browse topics/messages/consumer groups
- `docker compose exec postgres psql -U appuser -d orderdb -c "select * from orders;"` — inspect the data directly

Run the same POST a bunch of times — roughly 30% of orders will have their payment simulated-fail, which triggers the Saga's compensation path (inventory gets released, order gets cancelled). See [services/README.md](services/README.md) for exactly how that flow works.

## Learning path

Start with the practice guide ([MICROSERVICES_PRACTICE.md](MICROSERVICES_PRACTICE.md)) and use this implementation as the "First Milestone" checkpoint (section 31). From there, keep extending this codebase one exercise at a time (Outbox, DLQ, Saga Orchestrator, Kafka Streams, ...) before moving into the Kubernetes exercises.
