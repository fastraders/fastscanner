
# Redis Unix Socket vs TCP Access Benchmark Report (Target OS: Linux)

## Setup Steps for Redis Unix Socket Access

### 1. Enable Redis Unix Socket in `redis.conf`
Edit the Redis config file:

```bash
sudo nano /etc/redis/redis.conf
```

Add or ensure the following lines exist (uncommented):

```ini
unixsocket /run/redis/redis-server.sock
unixsocketperm 770
```

### 2. Restart Redis to Apply Changes

```bash
sudo systemctl restart redis
```

### 3. Add Your User to the `redis` Group

```bash
sudo usermod -aG redis $USER
newgrp redis
```

### 4. Verify the Socket Exists

```bash
ls -l /run/redis/redis-server.sock
```

Expected output:

```
srwxrwx--- 1 redis redis ...
```

### 5. Connect via Python Using aioredis

Update your Redis connection:

```python
redis = aioredis.Redis(
    unix_socket_path="/run/redis/redis-server.sock",
    decode_responses=True,
)
```

---

## Benchmark Comparison: TCP vs Unix Socket

### TCP (Before)
![TCP Access Benchmark]

- **AAPL:** 0.005229s
- **MSFT:** 0.003512s
- **GOOGL:** 0.003587s

### Unix Socket (After)
![Unix Socket Benchmark]

- **AAPL:** 0.001165s
- **MSFT:** 0.001416s
- **GOOGL:** 0.001473s

