#!/usr/bin/env python3
"""
Distributed Key-Value Store
Redis-compatible server implementation
"""

import asyncio
import json
import logging
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, asdict
from enum import Enum
import hashlib

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DataType(Enum):
    STRING = "string"
    LIST = "list"
    SET = "set"
    HASH = "hash"
    ZSET = "zset"


@dataclass
class KVEntry:
    """Key-value entry with metadata"""
    key: str
    value: Any
    dtype: DataType = DataType.STRING
    expires_at: Optional[float] = None
    version: int = 1
    
    def is_expired(self) -> bool:
        if self.expires_at is None:
            return False
        return asyncio.get_event_loop().time() > self.expires_at


class KeyValueStore:
    """In-memory key-value store with data structures"""
    
    def __init__(self):
        self.data: Dict[str, KVEntry] = {}
        self.lock = asyncio.Lock()
        self.subscribers: Dict[str, List[asyncio.Queue]] = {}
    
    # String operations
    async def set(self, key: str, value: str, ex: int = None) -> str:
        """SET key value [EX seconds]"""
        async with self.lock:
            expires = None
            if ex:
                expires = asyncio.get_event_loop().time() + ex
            
            self.data[key] = KVEntry(
                key=key,
                value=value,
                dtype=DataType.STRING,
                expires_at=expires
            )
            return "OK"
    
    async def get(self, key: str) -> Optional[str]:
        """GET key"""
        async with self.lock:
            entry = self.data.get(key)
            if not entry or entry.is_expired():
                if entry and entry.is_expired():
                    del self.data[key]
                return None
            return str(entry.value)
    
    async def delete(self, *keys: str) -> int:
        """DEL key1 key2 ..."""
        async with self.lock:
            count = 0
            for key in keys:
                if key in self.data:
                    del self.data[key]
                    count += 1
            return count
    
    async def exists(self, *keys: str) -> int:
        """EXISTS key1 key2 ..."""
        async with self.lock:
            return sum(1 for k in keys if k in self.data and not self.data[k].is_expired())
    
    async def incr(self, key: str) -> int:
        """INCR key"""
        async with self.lock:
            entry = self.data.get(key)
            if not entry:
                value = 1
            else:
                try:
                    value = int(entry.value) + 1
                except ValueError:
                    raise ValueError("Value is not an integer")
            
            self.data[key] = KVEntry(key=key, value=str(value), dtype=DataType.STRING)
            return value
    
    # List operations
    async def lpush(self, key: str, *values: str) -> int:
        """LPUSH key value1 value2 ..."""
        async with self.lock:
            entry = self.data.get(key)
            if not entry or entry.dtype != DataType.LIST:
                lst = []
            else:
                lst = list(entry.value)
            
            lst = list(values) + lst
            self.data[key] = KVEntry(key=key, value=lst, dtype=DataType.LIST)
            return len(lst)
    
    async def rpush(self, key: str, *values: str) -> int:
        """RPUSH key value1 value2 ..."""
        async with self.lock:
            entry = self.data.get(key)
            if not entry or entry.dtype != DataType.LIST:
                lst = []
            else:
                lst = list(entry.value)
            
            lst.extend(values)
            self.data[key] = KVEntry(key=key, value=lst, dtype=DataType.LIST)
            return len(lst)
    
    async def lrange(self, key: str, start: int, stop: int) -> List[str]:
        """LRANGE key start stop"""
        async with self.lock:
            entry = self.data.get(key)
            if not entry or entry.dtype != DataType.LIST:
                return []
            
            lst = entry.value
            # Handle negative indices
            if start < 0:
                start = max(0, len(lst) + start)
            if stop < 0:
                stop = len(lst) + stop + 1
            else:
                stop = stop + 1
            
            return lst[start:stop]
    
    # Set operations
    async def sadd(self, key: str, *members: str) -> int:
        """SADD key member1 member2 ..."""
        async with self.lock:
            entry = self.data.get(key)
            if not entry or entry.dtype != DataType.SET:
                s = set()
            else:
                s = set(entry.value)
            
            added = 0
            for member in members:
                if member not in s:
                    s.add(member)
                    added += 1
            
            self.data[key] = KVEntry(key=key, value=list(s), dtype=DataType.SET)
            return added
    
    async def smembers(self, key: str) -> List[str]:
        """SMEMBERS key"""
        async with self.lock:
            entry = self.data.get(key)
            if not entry or entry.dtype != DataType.SET:
                return []
            return list(entry.value)
    
    # Hash operations
    async def hset(self, key: str, field: str, value: str) -> int:
        """HSET key field value"""
        async with self.lock:
            entry = self.data.get(key)
            if not entry or entry.dtype != DataType.HASH:
                h = {}
            else:
                h = dict(entry.value)
            
            is_new = field not in h
            h[field] = value
            self.data[key] = KVEntry(key=key, value=h, dtype=DataType.HASH)
            return 1 if is_new else 0
    
    async def hgetall(self, key: str) -> Dict[str, str]:
        """HGETALL key"""
        async with self.lock:
            entry = self.data.get(key)
            if not entry or entry.dtype != DataType.HASH:
                return {}
            return dict(entry.value)
    
    # Pub/Sub
    async def subscribe(self, channel: str) -> asyncio.Queue:
        """Subscribe to channel"""
        queue = asyncio.Queue()
        if channel not in self.subscribers:
            self.subscribers[channel] = []
        self.subscribers[channel].append(queue)
        return queue
    
    async def publish(self, channel: str, message: str) -> int:
        """Publish message to channel"""
        subscribers = self.subscribers.get(channel, [])
        for queue in subscribers:
            await queue.put(message)
        return len(subscribers)
    
    # Info
    async def info(self) -> Dict[str, Any]:
        """Get server info"""
        async with self.lock:
            return {
                "keys": len(self.data),
                "uptime": "running",
                "version": "1.0.0"
            }


class RESPParser:
    """Redis Serialization Protocol parser"""
    
    @staticmethod
    def decode(data: bytes) -> List[str]:
        """Parse RESP protocol to command array"""
        try:
            text = data.decode('utf-8').strip()
            lines = text.split('\r\n')
            
            if not lines:
                return []
            
            # Simple string or error
            if lines[0][0] in ('+', '-'):
                return [lines[0][1:]]
            
            # Array (*3\r\n$3\r\nSET\r\n...)
            if lines[0][0] == '*':
                count = int(lines[0][1:])
                result = []
                idx = 1
                for _ in range(count):
                    if idx < len(lines) and lines[idx][0] == '$':
                        idx += 1
                        if idx < len(lines):
                            result.append(lines[idx])
                            idx += 1
                return result
            
            # Bulk string ($3\r\nkey\r\n)
            if lines[0][0] == '$':
                return [lines[1]] if len(lines) > 1 else []
            
            # Inline command (GET key)
            return text.split()
            
        except Exception as e:
            logger.error(f"Parse error: {e}")
            return []
    
    @staticmethod
    def encode(value: Any) -> bytes:
        """Encode response to RESP"""
        if value is None:
            return b"$-1\r\n"
        
        if isinstance(value, str):
            return f"${len(value)}\r\n{value}\r\n".encode()
        
        if isinstance(value, int):
            return f":{value}\r\n".encode()
        
        if isinstance(value, list):
            parts = [f"*{len(value)}\r\n".encode()]
            for item in value:
                parts.append(RESPParser.encode(item))
            return b"".join(parts)
        
        if value == "OK":
            return b"+OK\r\n"
        
        if isinstance(value, dict):
            items = []
            for k, v in value.items():
                items.extend([k, str(v)])
            return RESPParser.encode(items)
        
        return RESPParser.encode(str(value))


class KVServer:
    """TCP server for key-value store"""
    
    def __init__(self, host: str = "0.0.0.0", port: int = 6379):
        self.host = host
        self.port = port
        self.store = KeyValueStore()
        self.running = False
    
    async def handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        """Handle client connection"""
        addr = writer.get_extra_info('peername')
        logger.info(f"Client connected: {addr}")
        
        try:
            while self.running:
                data = await reader.read(4096)
                if not data:
                    break
                
                command = RESPParser.decode(data)
                if not command:
                    continue
                
                response = await self.execute_command(command)
                writer.write(RESPParser.encode(response))
                await writer.drain()
                
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.error(f"Client error: {e}")
        finally:
            writer.close()
            logger.info(f"Client disconnected: {addr}")
    
    async def execute_command(self, cmd: List[str]) -> Any:
        """Execute Redis command"""
        if not cmd:
            return None
        
        command = cmd[0].upper()
        args = cmd[1:]
        
        try:
            # String commands
            if command == "PING":
                return "PONG"
            
            if command == "SET" and len(args) >= 2:
                ex = None
                if len(args) >= 4 and args[2].upper() == "EX":
                    ex = int(args[3])
                return await self.store.set(args[0], args[1], ex)
            
            if command == "GET" and len(args) >= 1:
                return await self.store.get(args[0])
            
            if command == "DEL" and len(args) >= 1:
                return await self.store.delete(*args)
            
            if command == "EXISTS" and len(args) >= 1:
                return await self.store.exists(*args)
            
            if command == "INCR" and len(args) >= 1:
                return await self.store.incr(args[0])
            
            # List commands
            if command == "LPUSH" and len(args) >= 2:
                return await self.store.lpush(args[0], *args[1:])
            
            if command == "RPUSH" and len(args) >= 2:
                return await self.store.rpush(args[0], *args[1:])
            
            if command == "LRANGE" and len(args) >= 3:
                return await self.store.lrange(args[0], int(args[1]), int(args[2]))
            
            # Set commands
            if command == "SADD" and len(args) >= 2:
                return await self.store.sadd(args[0], *args[1:])
            
            if command == "SMEMBERS" and len(args) >= 1:
                return await self.store.smembers(args[0])
            
            # Hash commands
            if command == "HSET" and len(args) >= 3:
                return await self.store.hset(args[0], args[1], args[2])
            
            if command == "HGETALL" and len(args) >= 1:
                return await self.store.hgetall(args[0])
            
            # Info
            if command == "INFO":
                info = await self.store.info()
                return json.dumps(info)
            
            if command == "COMMAND":
                return ["SET", "GET", "DEL", "EXISTS", "INCR", "LPUSH", "RPUSH", 
                        "LRANGE", "SADD", "SMEMBERS", "HSET", "HGETALL", "INFO", "PING"]
            
            return f"-ERR unknown command '{command}'"
            
        except Exception as e:
            logger.error(f"Command error: {e}")
            return f"-ERR {str(e)}"
    
    async def start(self):
        """Start the server"""
        self.running = True
        server = await asyncio.start_server(
            self.handle_client, self.host, self.port
        )
        
        addr = server.sockets[0].getsockname()
        logger.info(f"🚀 KV Store server running on {addr}")
        logger.info(f"   Ready to accept connections on port {self.port}")
        
        async with server:
            await server.serve_forever()


async def main():
    """Main entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Distributed Key-Value Store")
    parser.add_argument("--host", default="0.0.0.0", help="Bind address")
    parser.add_argument("--port", type=int, default=6379, help="Port number")
    args = parser.parse_args()
    
    server = KVServer(host=args.host, port=args.port)
    
    try:
        await server.start()
    except KeyboardInterrupt:
        logger.info("Shutting down...")


if __name__ == "__main__":
    asyncio.run(main())
