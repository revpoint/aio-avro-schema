# -*- coding: utf-8 -*-

"""Top-level package for AIO Avro Schema."""

__author__ = """Jangl"""
__email__ = 'tech@jangl.com'
__version__ = '0.1.0'

from .error import ClientError
from .load import load, loads
from .cached_schema_registry_client import CachedSchemaRegistryClient
from .serializer import SerializerError, KeySerializerError, ValueSerializerError
from .serializer.message_serializer import MessageSerializer


class AvroSerializer:
    def __init__(self, schema_registry_url):
        self.schema_registry = CachedSchemaRegistryClient(schema_registry_url)
        self._serializer = MessageSerializer(self.schema_registry)

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    async def close(self):
        await self.schema_registry.close()

    async def decode_message(self, message, is_key=False):
        if message is None:
            return
        return await self._serializer.decode_message(message, is_key)

    async def encode_message(self, topic, schema, message, is_key=False):
        if message is None:
            return
        return await self._serializer.encode_record_with_schema(topic, schema, message, is_key)
