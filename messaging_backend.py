"""
Real-Time Event & Messaging Platform Backend

PostgreSQL DDL Statements:
--------------------------
CREATE TABLE chat_room (
    id BIGSERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    slug VARCHAR(255) UNIQUE NOT NULL,
    is_private BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE chat_room_participant (
    id BIGSERIAL PRIMARY KEY,
    room_id BIGINT REFERENCES chat_room(id) ON DELETE CASCADE,
    user_id BIGINT NOT NULL,
    joined_at TIMESTAMPTZ DEFAULT NOW(),
    last_ack_message_id BIGINT DEFAULT 0,
    CONSTRAINT unique_room_user UNIQUE (room_id, user_id)
);

CREATE TABLE chat_message (
    id BIGSERIAL PRIMARY KEY,
    room_id BIGINT REFERENCES chat_room(id) ON DELETE CASCADE,
    sender_id BIGINT NOT NULL,
    parent_message_id BIGINT REFERENCES chat_message(id) ON DELETE SET NULL,
    content TEXT NOT NULL,
    client_idempotency_key VARCHAR(128) NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    CONSTRAINT unique_sender_idempotency UNIQUE (sender_id, client_idempotency_key)
);

CREATE INDEX idx_chat_message_room_id_desc ON chat_message (room_id, id DESC);

CREATE TABLE chat_attachment (
    id BIGSERIAL PRIMARY KEY,
    message_id BIGINT REFERENCES chat_message(id) ON DELETE CASCADE,
    file_url VARCHAR(1024) NOT NULL,
    file_type VARCHAR(64) NOT NULL,
    file_size_bytes BIGINT NOT NULL,
    uploaded_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE chat_mention (
    id BIGSERIAL PRIMARY KEY,
    message_id BIGINT REFERENCES chat_message(id) ON DELETE CASCADE,
    mentioned_user_id BIGINT NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    CONSTRAINT unique_message_mention UNIQUE (message_id, mentioned_user_id)
);
"""

import json
from django.db import models, transaction
from channels.generic.websocket import AsyncJsonWebsocketConsumer
from channels.db import database_sync_to_async

# ==============================================================================
# Django ORM Models
# ==============================================================================

class ChatRoom(models.Model):
    name = models.CharField(max_length=255)
    slug = models.SlugField(max_length=255, unique=True)
    is_private = models.BooleanField(default=False)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = 'chat_room'

class ChatRoomParticipant(models.Model):
    room = models.ForeignKey(ChatRoom, on_delete=models.CASCADE, related_name='participants')
    user_id = models.BigIntegerField()
    joined_at = models.DateTimeField(auto_now_add=True)
    last_ack_message_id = models.BigIntegerField(default=0)

    class Meta:
        db_table = 'chat_room_participant'
        unique_together = ('room', 'user_id')

class ChatMessage(models.Model):
    room = models.ForeignKey(ChatRoom, on_delete=models.CASCADE, related_name='messages')
    sender_id = models.BigIntegerField()
    parent_message = models.ForeignKey('self', on_delete=models.SET_NULL, null=True, blank=True, related_name='replies')
    content = models.TextField()
    client_idempotency_key = models.CharField(max_length=128)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = 'chat_message'
        unique_together = ('sender_id', 'client_idempotency_key')
        indexes = [
            models.Index(fields=['room', '-id'], name='idx_chat_message_room_id_desc'),
        ]

class ChatAttachment(models.Model):
    message = models.ForeignKey(ChatMessage, on_delete=models.CASCADE, related_name='attachments')
    file_url = models.URLField(max_length=1024)
    file_type = models.CharField(max_length=64)
    file_size_bytes = models.BigIntegerField()
    uploaded_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = 'chat_attachment'

class ChatMention(models.Model):
    message = models.ForeignKey(ChatMessage, on_delete=models.CASCADE, related_name='mentions')
    mentioned_user_id = models.BigIntegerField()
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = 'chat_mention'
        unique_together = ('message', 'mentioned_user_id')


# ==============================================================================
# Transactional Messaging Engine
# ==============================================================================

class TransactionalMessagingEngine:
    @staticmethod
    @transaction.atomic
    def persist_message(room_slug, sender_id, content, idempotency_key, parent_id=None, mentions=None):
        room = ChatRoom.objects.get(slug=room_slug)
        
        # Idempotency check: Return existing message if key already exists for sender
        existing = ChatMessage.objects.filter(sender_id=sender_id, client_idempotency_key=idempotency_key).first()
        if existing:
            return existing, False

        message = ChatMessage.objects.create(
            room=room,
            sender_id=sender_id,
            content=content,
            client_idempotency_key=idempotency_key,
            parent_message_id=parent_id
        )

        if mentions:
            mention_objs = [
                ChatMention(message=message, mentioned_user_id=u_id)
                for u_id in mentions
            ]
            ChatMention.objects.bulk_create(mention_objs, ignore_conflicts=True)

        return message, True

    @staticmethod
    def get_missed_events(room_slug, after_id, limit=100):
        room = ChatRoom.objects.get(slug=room_slug)
        return list(
            ChatMessage.objects.filter(room=room, id__gt=after_id)
            .order_by('id')[:limit]
            .values('id', 'sender_id', 'content', 'parent_message_id', 'client_idempotency_key', 'created_at')
        )

    @staticmethod
    def update_participant_ack(room_slug, user_id, message_id):
        ChatRoomParticipant.objects.filter(
            room__slug=room_slug, user_id=user_id
        ).update(last_ack_message_id=message_id)


# ==============================================================================
# Asynchronous WebSocket Consumer
# ==============================================================================

class ChatEventConsumer(AsyncJsonWebsocketConsumer):
    async def connect(self):
        self.room_slug = self.scope['url_route']['kwargs']['room_slug']
        self.room_group_name = f"chat_{self.room_slug}"
        self.user_id = self.scope.get('user_id', 0)

        await self.channel_layer.group_add(
            self.room_group_name,
            self.channel_name
        )
        await self.accept()

    async def disconnect(self, close_code):
        await self.channel_layer.group_discard(
            self.room_group_name,
            self.channel_name
        )

    async def receive_json(self, content):
        event_type = content.get('event')

        if event_type == 'send_message':
            msg_obj, created = await database_sync_to_async(
                TransactionalMessagingEngine.persist_message
            )(
                room_slug=self.room_slug,
                sender_id=self.user_id,
                content=content.get('content'),
                idempotency_key=content.get('client_idempotency_key'),
                parent_id=content.get('parent_message_id'),
                mentions=content.get('mentions', [])
            )

            event_payload = {
                'type': 'chat_message_broadcast',
                'event': 'message.created',
                'message': {
                    'id': msg_obj.id,
                    'room_slug': self.room_slug,
                    'sender_id': msg_obj.sender_id,
                    'content': msg_obj.content,
                    'parent_message_id': msg_obj.parent_message_id,
                    'client_idempotency_key': msg_obj.client_idempotency_key,
                    'created_at': msg_obj.created_at.isoformat()
                }
            }
            await self.channel_layer.group_send(self.room_group_name, event_payload)

        elif event_type == 'sync_missed_events':
            last_ack_id = content.get('last_ack_message_id', 0)
            missed_messages = await database_sync_to_async(
                TransactionalMessagingEngine.get_missed_events
            )(self.room_slug, last_ack_id)

            await self.send_json({
                'event': 'sync.replay',
                'room_slug': self.room_slug,
                'messages': [
                    {
                        'id': m['id'],
                        'sender_id': m['sender_id'],
                        'content': m['content'],
                        'parent_message_id': m['parent_message_id'],
                        'client_idempotency_key': m['client_idempotency_key'],
                        'created_at': m['created_at'].isoformat() if hasattr(m['created_at'], 'isoformat') else str(m['created_at'])
                    } for m in missed_messages
                ]
            })

        elif event_type == 'ack_message':
            msg_id = content.get('message_id')
            await database_sync_to_async(
                TransactionalMessagingEngine.update_participant_ack
            )(self.room_slug, self.user_id, msg_id)

    async def chat_message_broadcast(self, event):
        await self.send_json(event)
