# Microservices + Kafka + Saga — Local Practice Guide

## Goal

Build a local event-driven microservices system that helps you practice:

- Microservice boundaries
- REST APIs
- Kafka producers and consumers
- Kafka partitions, offsets, consumer groups, and message keys
- Kafka Streams
- Saga pattern
- Saga choreography
- Saga orchestration
- Transactional Outbox
- Inbox / idempotent consumers
- Retry and Dead Letter Queue
- Eventual consistency
- PostgreSQL database-per-service
- Redis
- API Gateway
- Authentication with JWT
- OpenTelemetry tracing
- Prometheus + Grafana
- Docker Compose
- Failure recovery
- Basic distributed-system testing

---

# 1. Practice Project

Build an **Order Processing Platform**.

The system will contain:

```text
Client
  |
  v
API Gateway
  |
  v
Order Service
  |
  +------> Kafka
             |
     +-------+---------+-------------+
     |                 |             |
     v                 v             v
Inventory Service  Payment Service  Notification Service
     |                 |
     +--------+--------+
              |
              v
        Shipping Service
```

Main business flow:

```text
Create Order
    |
    v
Reserve Inventory
    |
    v
Process Payment
    |
    v
Create Shipment
    |
    v
Complete Order
```

Failure example:

```text
Create Order
    |
Reserve Inventory
    |
Payment Failed
    |
    v
Release Inventory
    |
    v
Cancel Order
```

---

# 2. Recommended Tech Stack

Use this stack initially:

```text
Language:
Go

HTTP Framework:
Gin / Fiber / net/http

Messaging:
Apache Kafka

Streaming:
Kafka Streams
```

Important:

Kafka Streams is a Java library.

If the main microservices are written in Go, create a small separate Java/Kotlin service for Kafka Streams practice.

Alternative for Go-only experimentation:

- franz-go
- confluent-kafka-go

But you should still practice real Kafka Streams separately because it is commonly discussed as a distinct Kafka technology.

Databases:

```text
PostgreSQL
Redis
```

Infrastructure:

```text
Docker
Docker Compose
```

Observability:

```text
OpenTelemetry
Prometheus
Grafana
Jaeger
```

Optional later:

```text
Debezium
Schema Registry
Kubernetes
```

---

# 3. Repository Structure

Create:

```text
microservices-lab/
│
├── docker-compose.yml
├── README.md
├── .env
│
├── gateway/
│
├── services/
│   ├── order-service/
│   ├── inventory-service/
│   ├── payment-service/
│   ├── shipping-service/
│   ├── notification-service/
│   └── saga-orchestrator/
│
├── kafka-streams/
│   └── order-analytics/
│
├── infra/
│   ├── kafka/
│   ├── postgres/
│   ├── monitoring/
│   └── otel/
│
└── scripts/
```

---

# 4. Local Infrastructure

Your Docker Compose environment should eventually contain:

```text
Kafka
Kafka UI
PostgreSQL
Redis
Schema Registry
Prometheus
Grafana
Jaeger
OpenTelemetry Collector
```

Start small.

Phase 1:

```text
Kafka
Kafka UI
PostgreSQL
Redis
```

Phase 2:

```text
Schema Registry
```

Phase 3:

```text
Prometheus
Grafana
Jaeger
OpenTelemetry
```

Phase 4:

```text
Debezium
```

---

# 5. Microservices

## Order Service

Responsibilities:

- Create order
- Update order status
- Cancel order
- Publish OrderCreated
- Consume Saga events

Database:

```text
orders
order_items
outbox_events
processed_messages
```

Example statuses:

```text
PENDING
INVENTORY_RESERVED
PAYMENT_COMPLETED
SHIPPING_CREATED
COMPLETED
CANCELLED
FAILED
```

---

## Inventory Service

Responsibilities:

- Reserve stock
- Release stock
- Track available quantity

Consumes:

```text
OrderCreated
ReleaseInventoryRequested
```

Publishes:

```text
InventoryReserved
InventoryReservationFailed
InventoryReleased
```

Database:

```text
products
inventory
reservations
outbox_events
processed_messages
```

---

## Payment Service

Responsibilities:

- Simulate payment
- Support success/failure
- Refund payment

Consumes:

```text
InventoryReserved
RefundPaymentRequested
```

Publishes:

```text
PaymentCompleted
PaymentFailed
PaymentRefunded
```

Database:

```text
payments
refunds
outbox_events
processed_messages
```

Do not integrate a real payment gateway for this learning project.

Simulate payment results.

---

## Shipping Service

Responsibilities:

- Create shipment
- Cancel shipment

Consumes:

```text
PaymentCompleted
```

Publishes:

```text
ShipmentCreated
ShipmentFailed
ShipmentCancelled
```

---

## Notification Service

Consumes events such as:

```text
OrderCreated
PaymentCompleted
PaymentFailed
OrderCompleted
OrderCancelled
```

For local practice, just log messages.

Example:

```text
EMAIL: Order #123 was completed.
```

---

# 6. Kafka Topics

Start with:

```text
orders
inventory
payments
shipping
notifications
```

Later use clearer event-specific topics if desired:

```text
order.events
inventory.events
payment.events
shipping.events
```

Retry topics:

```text
payment.retry
inventory.retry
```

Dead Letter topics:

```text
payment.dlq
inventory.dlq
order.dlq
```

---

# 7. Event Structure

Use a common event envelope.

Example:

```json
{
  "eventId": "uuid",
  "eventType": "OrderCreated",
  "aggregateId": "order-123",
  "correlationId": "uuid",
  "causationId": "uuid",
  "timestamp": "2026-01-01T10:00:00Z",
  "version": 1,
  "payload": {}
}
```

Important fields:

```text
eventId
eventType
aggregateId
correlationId
timestamp
version
payload
```

Use:

```text
orderId
```

as the Kafka message key for order-related events.

This keeps events for the same order in the same Kafka partition.

---

# 8. Exercise 1 — Basic Producer / Consumer

Create:

```text
Order Service
Inventory Service
```

Flow:

```text
POST /orders
     |
     v
Order Service
     |
 OrderCreated
     |
     v
Kafka
     |
     v
Inventory Service
```

Verify:

1. Order is written to PostgreSQL.
2. OrderCreated is published.
3. Inventory Service consumes it.
4. Inventory Service logs the event.

Do not implement Saga yet.

---

# 9. Exercise 2 — Kafka Consumer Groups

Run three Inventory Service instances:

```text
inventory-1
inventory-2
inventory-3
```

Use the same consumer group:

```text
inventory-service
```

Send multiple orders.

Observe:

```text
partition 0 -> consumer 1
partition 1 -> consumer 2
partition 2 -> consumer 3
```

Practice:

- consumer groups
- partitions
- rebalancing
- offsets

Then stop one consumer and observe rebalance behavior.

---

# 10. Exercise 3 — Message Ordering

Publish:

```text
OrderCreated
OrderUpdated
OrderCancelled
```

Use:

```text
orderId
```

as Kafka key.

Verify events for the same order remain ordered within a partition.

Understand:

Kafka guarantees ordering **inside a partition**, not globally across the whole topic.

---

# 11. Exercise 4 — Idempotency

Kafka consumers may receive messages more than once.

Create:

```text
processed_messages
```

Example:

```text
event_id UUID PRIMARY KEY
processed_at TIMESTAMP
```

Consumer logic:

```text
Receive event

Check processed_messages

If event already exists:
    skip

Otherwise:
    begin transaction

    process event
    save eventId

    commit
```

Test by manually sending the same event twice.

Expected result:

```text
business operation happens once
```

---

# 12. Exercise 5 — Retry

Make Payment Service randomly fail.

Example:

```text
30% simulated failure
```

Implement:

```text
Attempt 1
   |
fail
   |
Retry
   |
Attempt 2
   |
fail
   |
Retry
   |
Attempt 3
```

Use exponential backoff conceptually:

```text
1 second
5 seconds
20 seconds
```

Do not block a Kafka consumer thread by sleeping for long periods in a production-style implementation.

Prefer retry topics or scheduled retry handling.

---

# 13. Exercise 6 — Dead Letter Queue

After maximum retries:

```text
payment.events
     |
     v
Payment Consumer
     |
3 failures
     |
     v
payment.dlq
```

DLQ event should contain:

```text
originalEvent
failureReason
attemptCount
failedAt
service
```

Create an endpoint:

```text
GET /admin/dlq
```

Optional:

```text
POST /admin/dlq/{eventId}/retry
```

---

# 14. Exercise 7 — Saga Choreography

Implement:

```text
OrderCreated
     |
     v
Inventory Service
     |
InventoryReserved
     |
     v
Payment Service
     |
PaymentCompleted
     |
     v
Shipping Service
     |
ShipmentCreated
     |
     v
Order Service
     |
OrderCompleted
```

No central Saga coordinator exists.

Each service reacts to events.

---

# 15. Saga Compensation

Simulate:

```text
PaymentFailed
```

Compensation flow:

```text
PaymentFailed
     |
     v
Inventory Service
     |
ReleaseInventory
     |
     v
Order Service
     |
CancelOrder
```

Also try:

```text
ShipmentFailed
```

Possible compensation:

```text
ShipmentFailed
     |
     +--> Refund Payment
     |
     +--> Release Inventory
     |
     +--> Cancel Order
```

This is one of the most important exercises.

---

# 16. Exercise 8 — Saga Orchestration

Create:

```text
saga-orchestrator
```

Flow:

```text
                   Saga Orchestrator
                          |
                  Reserve Inventory
                          |
                          v
                    Inventory
                          |
                     success
                          |
                          v
                    Charge Payment
                          |
                          v
                     Payment
                          |
                     success
                          |
                          v
                   Create Shipment
```

On failure:

```text
Payment Failed
      |
      v
Saga Orchestrator
      |
      +--> Release Inventory
      |
      +--> Cancel Order
```

Persist Saga state.

Example:

```text
sagas

id
order_id
state
current_step
status
created_at
updated_at
```

Possible states:

```text
STARTED
INVENTORY_RESERVED
PAYMENT_COMPLETED
SHIPMENT_CREATED
COMPLETED
COMPENSATING
FAILED
```

---

# 17. Exercise 9 — Transactional Outbox

Incorrect approach:

```text
BEGIN

INSERT order

COMMIT

publish Kafka event
```

Problem:

```text
DB commit succeeds
Kafka publish fails
```

Now the database says the order exists, but other services never receive OrderCreated.

Implement Outbox:

```text
BEGIN

INSERT INTO orders
INSERT INTO outbox_events

COMMIT
```

Example table:

```sql
CREATE TABLE outbox_events (
    id UUID PRIMARY KEY,
    aggregate_id VARCHAR(100) NOT NULL,
    event_type VARCHAR(100) NOT NULL,
    payload JSONB NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    published_at TIMESTAMP NULL
);
```

Background worker:

```text
Read unpublished outbox rows
        |
        v
Publish Kafka
        |
        v
Mark published_at
```

Later replace polling with:

```text
Debezium CDC
```

---

# 18. Exercise 10 — Kafka Streams

Create a separate Kafka Streams application.

Input:

```text
order.events
```

Exercise A:

```text
Orders
   |
filter total > 1000
   |
   v
high-value-orders
```

Exercise B:

```text
Orders
   |
groupBy(customerId)
   |
count
   |
customer-order-count
```

Exercise C:

```text
orders
   +
payments
   |
 JOIN
   |
   v
paid-orders
```

Practice:

```text
KStream
KTable
GlobalKTable
map
filter
groupByKey
aggregate
join
windowing
state stores
```

---

# 19. Exercise 11 — Schema Registry

Initially JSON is fine.

Later introduce:

```text
Avro
```

or:

```text
Protobuf
```

Practice schema evolution.

Version 1:

```json
{
  "orderId": "123",
  "amount": 100
}
```

Version 2:

```json
{
  "orderId": "123",
  "amount": 100,
  "currency": "USD"
}
```

Understand:

```text
Backward compatibility
Forward compatibility
Full compatibility
```

---

# 20. Exercise 12 — Redis

Use Redis for several small exercises.

## Cache

```text
GET /products/{id}

Redis
   |
miss
   |
PostgreSQL
```

## Rate Limiting

Example:

```text
100 requests / minute / client
```

## Distributed Lock

Practice:

```text
SET key value NX PX ttl
```

But also study the problems:

- lock owner crashes
- TTL expires while work continues
- another worker acquires the lock
- old worker still thinks it owns the lock
- safe unlock
- fencing tokens

Do not assume a basic Redis lock solves every distributed locking problem.

---

# 21. Exercise 13 — API Gateway

Add:

```text
gateway
```

External API:

```text
POST /api/orders
GET /api/orders/:id
GET /api/products/:id
```

Gateway responsibilities:

```text
Routing
Authentication
Rate limiting
Correlation ID
Request logging
```

Do not put core business logic in the gateway.

---

# 22. Exercise 14 — Authentication

Implement simple JWT authentication.

Flow:

```text
Login
   |
   v
JWT
   |
   v
API Gateway
   |
validate
   |
   v
Microservice
```

Include claims such as:

```json
{
  "sub": "user-123",
  "roles": ["customer"]
}
```

Practice:

```text
authentication
authorization
RBAC
token expiration
```

---

# 23. Exercise 15 — Timeouts and Circuit Breakers

For REST calls:

```text
Order Service
     |
     v
Pricing Service
```

Simulate Pricing Service being slow.

Never allow requests to wait indefinitely.

Practice:

```text
Timeout
Retry
Circuit Breaker
Fallback
```

Understand:

Retry is not always safe.

Do not automatically retry non-idempotent operations unless the operation has an idempotency strategy.

---

# 24. Exercise 16 — Observability

Every request should carry:

```text
traceId
correlationId
```

Example:

```text
Client request

traceId=abc

Gateway
   |
Order Service
   |
Kafka
   |
Inventory
   |
Payment
```

Use OpenTelemetry.

View traces using:

```text
Jaeger
```

Metrics using:

```text
Prometheus
Grafana
```

Track:

```text
HTTP request count
HTTP latency
Kafka consumer lag
failed events
retry count
DLQ count
payment failures
order completion time
```

---

# 25. Exercise 17 — Failure Testing

This is extremely important.

Do not only test the happy path.

Test:

```text
Kafka unavailable
PostgreSQL unavailable
Redis unavailable
Payment Service crashes
Inventory Service crashes
Consumer crashes after DB commit
Consumer crashes before offset commit
Duplicate message
Out-of-order message
Slow consumer
Bad event schema
DLQ event
Network timeout
Saga compensation failure
```

For each failure ask:

```text
What happens?

Can data be lost?

Can data be duplicated?

Can the operation safely retry?

How is the system recovered?
```

---

# 26. Docker Commands

Start everything:

```bash
docker compose up -d
```

Check containers:

```bash
docker compose ps
```

View logs:

```bash
docker compose logs -f
```

Specific service:

```bash
docker compose logs -f order-service
```

Stop:

```bash
docker compose down
```

Delete containers and volumes:

```bash
docker compose down -v
```

Be careful:

```text
-v
```

deletes local persisted Docker volumes.

---

# 27. Kafka Commands to Practice

List topics:

```bash
kafka-topics --bootstrap-server localhost:9092 --list
```

Describe topic:

```bash
kafka-topics \
  --bootstrap-server localhost:9092 \
  --describe \
  --topic order.events
```

Produce manually:

```bash
kafka-console-producer \
  --bootstrap-server localhost:9092 \
  --topic order.events
```

Consume:

```bash
kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic order.events \
  --from-beginning
```

Consumer groups:

```bash
kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --list
```

Describe group:

```bash
kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --describe \
  --group inventory-service
```

If Kafka runs only inside Docker, run these commands inside the Kafka container or install Kafka CLI tools locally.

---

# 28. Important Concepts You Must Be Able to Explain

## Microservices

```text
service boundaries
database per service
loose coupling
independent deployment
sync vs async communication
```

## Kafka

```text
broker
topic
partition
offset
producer
consumer
consumer group
message key
rebalance
replication
retention
compaction
```

## Delivery Semantics

Understand:

```text
At-most-once
At-least-once
Exactly-once
```

Do not say:

```text
Kafka guarantees every business operation executes exactly once.
```

Exactly-once processing requires careful end-to-end design.

---

# 29. Essential Distributed-System Patterns

Practice and understand:

```text
Saga
Transactional Outbox
Inbox Pattern
Idempotent Consumer
Retry
Dead Letter Queue
Circuit Breaker
Timeout
Bulkhead
API Gateway
Database per Service
Event Sourcing
CQRS
CDC
Optimistic Locking
Distributed Locking
```

You do not need to implement all of them in the first version.

---

# 30. Recommended Learning Order

Follow this sequence.

```text
01. Docker Compose
02. Two basic microservices
03. PostgreSQL database per service
04. REST communication
05. Kafka producer
06. Kafka consumer
07. Kafka partitions
08. Consumer groups
09. Message ordering
10. Idempotent consumer
11. Retry
12. DLQ
13. Saga choreography
14. Saga compensation
15. Saga orchestration
16. Transactional Outbox
17. Debezium CDC
18. Kafka Streams
19. Schema Registry
20. Redis
21. API Gateway
22. JWT authentication
23. Circuit breaker
24. OpenTelemetry
25. Prometheus
26. Grafana
27. Failure testing
28. Kubernetes
```

Do not start Kubernetes before understanding the application-level distributed-system problems.

---

# 31. First Milestone

Your first milestone should contain only:

```text
Kafka
Kafka UI
PostgreSQL
Redis

Order Service
Inventory Service
Payment Service
```

Flow:

```text
POST /orders
      |
      v
Order Service
      |
 OrderCreated
      |
      v
Kafka
      |
      v
Inventory Service
      |
 InventoryReserved
      |
      v
Kafka
      |
      v
Payment Service
      |
 PaymentCompleted
      |
      v
Kafka
      |
      v
Order Service
      |
      v
COMPLETED
```

When this works, intentionally make Payment fail.

Expected:

```text
PaymentFailed
     |
     v
ReleaseInventory
     |
     v
OrderCancelled
```

If you can build this correctly, you already have a useful local environment for learning Kafka and Saga.

---

# 32. Second Milestone

Add:

```text
Transactional Outbox
Idempotent Consumer
Retry
DLQ
```

Make sure the system survives:

```text
duplicate messages
consumer crashes
temporary service failure
temporary Kafka failure
```

---

# 33. Third Milestone

Add:

```text
Saga Orchestrator
Kafka Streams
Schema Registry
OpenTelemetry
Prometheus
Grafana
```

Now the project becomes much closer to a senior backend/system-design practice lab.

---

# 34. Final Milestone

Move the services from Docker Compose to local Kubernetes using either:

```text
kind
```

or:

```text
minikube
```

Practice:

```text
Deployments
Services
ConfigMaps
Secrets
readiness probes
liveness probes
resource limits
horizontal scaling
rolling updates
Jobs
CronJobs
```

Do this only after the Docker Compose version is working correctly.

---

# 34.5 Kubernetes Practice Lab

Once your Docker Compose version is stable, move into Kubernetes practice on a local cluster.

## Goal

Practice deploying, connecting, scaling, and debugging a small event-driven system inside Kubernetes.

You will learn:

```text
kubectl basics
kind / minikube setup
Deployments
Services
ConfigMaps
Secrets
Ingress
readiness and liveness probes
resource requests / limits
rolling updates
horizontal scaling
Jobs and CronJobs
kubectl logs, describe, exec
```

---

## Recommended Local Tools

Use one of these:

```text
kind
minikube
kubectl
Docker
```

### kind quick setup

```bash
kind create cluster --name microservices-lab
kubectl cluster-info
```

### minikube quick setup

```bash
minikube start
kubectl get nodes
```

---

## Minimal Kubernetes Practice Project

Create a simple deployment flow:

```text
Client
  |
  v
Ingress or LoadBalancer Service
  |
  v
API Gateway
  |
  v
Order Service
  |
  +-> Kafka
  |
  +-> PostgreSQL
```

Start with only these resources:

```text
1 Deployment for Order Service
1 Deployment for Inventory Service
1 Service for Order Service
1 Service for Inventory Service
1 ConfigMap for app settings
1 Secret for database credentials
```

Then add:

```text
Kafka Deployment / StatefulSet
Kafka Service
PostgreSQL Deployment / StatefulSet
Redis Deployment / Service
Prometheus / Grafana
```

---

## First Kubernetes Exercise

Create and deploy a simple app.

Example Deployment:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: order-service
spec:
  replicas: 2
  selector:
    matchLabels:
      app: order-service
  template:
    metadata:
      labels:
        app: order-service
    spec:
      containers:
        - name: order-service
          image: order-service:latest
          ports:
            - containerPort: 8080
          env:
            - name: PORT
              value: "8080"
```

Example Service:

```yaml
apiVersion: v1
kind: Service
metadata:
  name: order-service
spec:
  selector:
    app: order-service
  ports:
    - protocol: TCP
      port: 80
      targetPort: 8080
```

Practice commands:

```bash
kubectl apply -f order-service.yaml
kubectl get pods
kubectl get svc
kubectl logs deploy/order-service
```

---

## Exercise 2 — ConfigMaps and Secrets

Create a ConfigMap for environment settings:

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: order-service-config
data:
  APP_ENV: local
  KAFKA_BROKER: kafka:9092
```

Create a Secret for sensitive values:

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: order-service-secret
type: Opaque
stringData:
  DB_PASSWORD: secret
```

Inject them into the pod and verify they are available.

Practice:

```bash
kubectl get configmap
kubectl get secret
kubectl describe pod <pod-name>
```

---

## Exercise 3 — Readiness and Liveness Probes

Add probes to your service so Kubernetes knows when it is healthy.

Example:

```yaml
readinessProbe:
  httpGet:
    path: /health
    port: 8080
  initialDelaySeconds: 5
  periodSeconds: 10

livenessProbe:
  httpGet:
    path: /health
    port: 8080
  initialDelaySeconds: 15
  periodSeconds: 20
```

Understand the difference between:

```text
readiness probe = is ready to receive traffic?
liveness probe = is the container still alive?
```

---

## Exercise 4 — Scaling and Rolling Updates

Practice scaling the deployment:

```bash
kubectl scale deployment order-service --replicas=3
kubectl get pods -w
```

Practice a rolling update:

```bash
kubectl set image deployment/order-service order-service=order-service:v2
kubectl rollout status deployment/order-service
kubectl rollout history deployment/order-service
```

Observe:

```text
old pods stay alive until new pods are ready
traffic shifts gradually
```

---

## Exercise 5 — Troubleshooting

Learn these commands well:

```bash
kubectl get pods
kubectl get events
kubectl describe pod <pod-name>
kubectl logs <pod-name>
kubectl exec -it <pod-name> -- sh
kubectl get svc
kubectl get ingress
```

Typical questions to answer:

```text
Why is a pod not starting?
Why is a service not routing traffic?
Why is a readiness probe failing?
Why is the app not seeing the ConfigMap or Secret?
```

---

## Exercise 6 — Jobs and CronJobs

Create a one-time job for a migration or cleanup task:

```yaml
apiVersion: batch/v1
kind: Job
metadata:
  name: db-migration
spec:
  template:
    spec:
      restartPolicy: Never
      containers:
        - name: migration
          image: migration-image:latest
```

Create a CronJob for periodic tasks:

```yaml
apiVersion: batch/v1
kind: CronJob
metadata:
  name: cleanup-job
spec:
  schedule: "0 * * * *"
  jobTemplate:
    spec:
      template:
        spec:
          restartPolicy: OnFailure
          containers:
            - name: cleanup
              image: cleanup-image:latest
```

---

## Kubernetes Practice Order

Follow this sequence:

```text
01. kind or minikube setup
02. kubectl basics
03. Deploy a simple app
04. Service and DNS
05. ConfigMaps and Secrets
06. Readiness / liveness probes
07. Resource requests and limits
08. Horizontal scaling
09. Rolling updates and rollbacks
10. Logs, describe, exec, events
11. Jobs and CronJobs
12. Ingress
13. Helm basics
14. StatefulSets for Kafka / PostgreSQL
15. Failure simulation and recovery
```

---

## Important Kubernetes Interview Questions

Be ready to answer:

```text
What is the difference between a Deployment and a StatefulSet?
Why do we use Services in Kubernetes?
What happens when a pod is restarted?
Why do readiness and liveness probes matter?
How does Kubernetes handle rolling updates?
What is the difference between ConfigMap and Secret?
How do you debug a pod that is CrashLoopBackOff?
What is the role of a Service selector?
Why do we need resource requests and limits?
How does horizontal scaling work?
```

---

## Practical Tip

Do not start with a huge production-style cluster.

Start with:

```text
1 pod
1 service
1 deployment
1 configmap
1 secret
```

Then add one concept at a time.

When your Docker Compose microservices are working well, port the same architecture to Kubernetes and test the same failure scenarios there.

---

# 35. Interview Questions to Ask Yourself

While building, be able to answer:

```text
Why Kafka instead of REST?

Why Saga instead of a distributed database transaction?

What happens when a consumer processes an event twice?

What happens when DB commit succeeds but Kafka publish fails?

Why do we need the Outbox pattern?

What happens if compensation fails?

What happens when a consumer dies before committing its offset?

How do Kafka partitions affect ordering?

How do consumer groups scale processing?

What causes consumer group rebalancing?

How do you safely retry an event?

When should an event go to DLQ?

What is eventual consistency?

What is the difference between KStream and KTable?

Why use correlation IDs?

How do you trace an event across five services?

How do you handle incompatible event schema changes?
```

If you can build the system and clearly answer those questions, you will have strong practical understanding of event-driven microservices.

---

# Suggested Starting Point

Start with only:

```text
Order Service
Inventory Service
Kafka
PostgreSQL
Docker Compose
```

Do not build all components at once.

Your first target is:

```text
POST /orders
      ↓
OrderCreated
      ↓
Kafka
      ↓
Inventory Service
      ↓
InventoryReserved
```

After that works, add one distributed-system concept at a time.
