# Real-Time Event & Messaging Platform

## Overview
**Real-Time Event & Messaging Platform** is a low-latency, scalable messaging backend built to handle real-time bi-directional communications, atomic state synchronization, and robust connection-drop recovery.

## Tech Stack
- **Languages & Frameworks**: Python, Django Channels
- **Protocols & Communication**: WebSockets, REST APIs
- **Database & Persistence**: PostgreSQL (SQL), Redis (Channel Layer)

## Key Engineering Highlights
- **Low-Latency Event Persistence**: Integrated ASGI event loop with Redis channel layer and PostgreSQL database engine.
- **Strict Idempotency & Threading**: Enforced unique constraints (`UNIQUE (sender_id, client_idempotency_key)`) and self-referential relations for threaded message hierarchies.
- **Connection Drop Recovery**: Built-in cursor replay (`last_ack_message_id`) and delta-sync REST endpoints ensuring zero message loss across network disruptions.
- **Transactional Consistency**: Database writes encapsulated inside `transaction.atomic()` for message events, mentions, and file attachment metadata.

## Documentation
For complete technical specifications, relational schema design, WebSocket protocol definitions, and recovery flow diagrams, see the [Documentation](./DOCUMENTATION.md).
