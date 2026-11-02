# Real-Time Event & Messaging Platform — Technical Documentation

## 1. System Architecture & Event Persistence Flow

```
+------------------+         +--------------------+         +-----------------------+
| WebSocket Client | <-----> |    ASGI Server     | <-----> |  Redis Channel Layer  |
+------------------+         |  (Daphne/Uvicorn)  |         +-----------------------+
                             +--------------------+
                                       |
                                       v
                             +--------------------+         +-----------------------+
                             |  Django Channels   | <-----> |     PostgreSQL DB     |
                             |     Consumers      |         |  (Transactional Store)|
                             +--------------------+         +-----------------------+
```

### Event Persistence Flow:
1. **Client Request**: Client sends a JSON event (e.g., `send_message`) containing `client_idempotency_key` over a WebSocket connection.
2. **ASGI & Consumer handling**: The ASGI server routes the connection to `ChatEventConsumer`.
3. **Transactional Persistence**: Inside `transaction.atomic()`, the message is saved to PostgreSQL (`chat_message` table). If `client_idempotency_key` was already processed for this sender, the duplicate is ignored via unique constraints.
4. **Pub/Sub Broadcast**: Upon successful commit, the message event is published to the Redis Channel Layer for distribution to all online participants in the channel group.
5. **Acknowledgment & Delivery**: A `message.created` or `ack_message` event is returned to the client containing the database-assigned sequence ID (`id`).

---

## 2. Relational Database Schema (3NF)

### Tables & Key Attributes

#### `chat_room`
- `id` (BIGINT, PRIMARY KEY, AUTO-INCREMENT)
- `name` (VARCHAR(255), NOT NULL)
- `slug` (VARCHAR(255), UNIQUE, NOT NULL)
- `is_private` (BOOLEAN, DEFAULT FALSE)
- `created_at` (TIMESTAMPTZ, DEFAULT NOW())

#### `chat_room_participant`
- `id` (BIGINT, PRIMARY KEY, AUTO-INCREMENT)
- `room_id` (BIGINT, FOREIGN KEY -> `chat_room.id` ON DELETE CASCADE)
- `user_id` (BIGINT, NOT NULL)
- `joined_at` (TIMESTAMPTZ, DEFAULT NOW())
- `last_ack_message_id` (BIGINT, DEFAULT 0) -- Cursor for read receipts and delta sync tracking
- `UNIQUE (room_id, user_id)`

#### `chat_message`
- `id` (BIGINT, PRIMARY KEY, AUTO-INCREMENT)
- `room_id` (BIGINT, FOREIGN KEY -> `chat_room.id` ON DELETE CASCADE)
- `sender_id` (BIGINT, NOT NULL)
- `parent_message_id` (BIGINT, NULLABLE, FOREIGN KEY -> `chat_message.id` ON DELETE SET NULL) -- Self-referential for threaded replies
- `content` (TEXT, NOT NULL)
- `client_idempotency_key` (VARCHAR(128), NOT NULL)
- `created_at` (TIMESTAMPTZ, DEFAULT NOW())
- **Constraints & Indexes**:
  - `UNIQUE (sender_id, client_idempotency_key)`
  - `INDEX idx_chat_message_room_id_desc (room_id, id DESC)`

#### `chat_attachment`
- `id` (BIGINT, PRIMARY KEY, AUTO-INCREMENT)
- `message_id` (BIGINT, FOREIGN KEY -> `chat_message.id` ON DELETE CASCADE)
- `file_url` (VARCHAR(1024), NOT NULL)
- `file_type` (VARCHAR(64), NOT NULL)
- `file_size_bytes` (BIGINT, NOT NULL)
- `uploaded_at` (TIMESTAMPTZ, DEFAULT NOW())

#### `chat_mention`
- `id` (BIGINT, PRIMARY KEY, AUTO-INCREMENT)
- `message_id` (BIGINT, FOREIGN KEY -> `chat_message.id` ON DELETE CASCADE)
- `mentioned_user_id` (BIGINT, NOT NULL)
- `created_at` (TIMESTAMPTZ, DEFAULT NOW())
- `UNIQUE (message_id, mentioned_user_id)`

---

## 3. WebSocket JSON Event Protocol & REST API

### WebSocket JSON Events

#### 1. `send_message` (Client -> Server)
```json
{
  "event": "send_message",
  "room_slug": "general",
  "client_idempotency_key": "uuid-v4-12345",
  "content": "Hello team!",
  "parent_message_id": null
}
```

#### 2. `ack_message` (Client -> Server)
```json
{
  "event": "ack_message",
  "room_slug": "general",
  "message_id": 452
}
```

#### 3. `sync_missed_events` (Client -> Server)
```json
{
  "event": "sync_missed_events",
  "room_slug": "general",
  "last_ack_message_id": 450
}
```

#### 4. `message.created` (Server -> Broadcast)
```json
{
  "event": "message.created",
  "message": {
    "id": 451,
    "room_slug": "general",
    "sender_id": 12,
    "content": "Hello team!",
    "parent_message_id": null,
    "client_idempotency_key": "uuid-v4-12345",
    "created_at": "2026-11-02T10:00:00Z"
  }
}
```

#### 5. `sync.replay` (Server -> Client)
```json
{
  "event": "sync.replay",
  "room_slug": "general",
  "messages": [ ... ]
}
```

### REST API Delta-Sync Endpoint
- **Endpoint**: `GET /api/rooms/{slug}/messages/sync/?after_id={last_ack_message_id}`
- **Response**:
```json
{
  "room_slug": "general",
  "messages": [
    {
      "id": 451,
      "sender_id": 12,
      "content": "Hello team!",
      "parent_message_id": null,
      "created_at": "2026-11-02T10:00:00Z"
    }
  ],
  "has_more": false
}
```

---

## 4. State Synchronization & Connection Drop Recovery Design

1. **Transactional Guarantee (`transaction.atomic()`)**: All incoming messages and mentions/attachments are wrapped inside a single atomic database transaction. Idempotency is guaranteed via `UNIQUE (sender_id, client_idempotency_key)`.
2. **Cursor-Based Reconnection Replay**: Each client tracks its `last_ack_message_id`. Upon WebSocket reconnection, the client automatically emits `sync_missed_events` or queries `GET /api/rooms/{slug}/messages/sync/?after_id={last_ack_message_id}` to retrieve missed events in sequential order without duplication.
