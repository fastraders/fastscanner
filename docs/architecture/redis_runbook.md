# Redis UNIX Socket and AOF Setup Guide

## 🔌 Enabling Redis UNIX Socket Access

### 1. Edit the Redis Configuration File

```bash

1.Open the Redis config file:
sudo nano /etc/redis/redis.conf


2.Add or ensure the following lines are uncommented and present:
unixsocket /tmp/redis-server.sock
unixsocketperm 770

Restart Redis to Apply Changes
sudo systemctl restart redis


3. Add Your User to the redis Group
This grants your user access to the socket file:
sudo usermod -aG redis $USER
newgrp redis

4.Verify the UNIX Socket Exists
ls -l /tmp/redis-server.sock

Expected output:
srwxrwx--- 1 redis redis ...

5. Connect via Python Using aioredis
Update your Redis client initialization:

import redis.asyncio as aioredis
redis = aioredis.Redis(
    unix_socket_path="/tmp/redis-server.sock",
    decode_responses=True,
)


-----------------------------------------------------------------------------------------
Enabling AOF Persistence with appendfsync everysec
1. Open Redis Configuration
sudo nano /etc/redis/redis.conf


2. Enable AOF and Set Sync Strategy
Uncomment or add the following lines:
appendonly yes
appendfsync everysec



3. Save Changes and Restart Redis
sudo systemctl restart redis


------------------------------------------------------------------------------------------------------
Running FastScanner Benchmark Code
To benchmark Redis stream performance, we always start the reader before the writer:

# Start the Redis stream reader
poetry run python -m fastscanner.benchmarks.benchmark_redis_read

# Then run the Polygon stream writer
poetry run python -m fastscanner.benchmarks.benchmark_polygon_realtime

```
