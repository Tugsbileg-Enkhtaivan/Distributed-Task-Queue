# Distributed Task Queue

Standalone Node.js/TypeScript queue: Redis for waiting, delayed, and active jobs; PostgreSQL for completion audit logs and the dead-letter queue.

## Architecture

- `POST /jobs` enqueues work (`HSET` metadata, `LPUSH` waiting or `ZADD` delayed).
- Workers claim jobs atomically (`RPOPLPUSH` / `BLMOVE` plus Lua `HSET` to `active`).
- Failures retry with exponential backoff (`2^attempts` seconds) via the delayed ZSET.
- Exhausted jobs are marked `failed` and written to `job_logs` (DLQ). Completed jobs are audited there too.
- Worker heartbeats (`worker:{id}:heartbeat`) let a sweeper re-queue jobs from crashed processes.

## Prerequisites (Windows 11)

Redis in WSL:

```bash
sudo apt update
sudo apt install redis-server -y
sudo service redis-server start
redis-cli ping
```

PostgreSQL (local or WSL). Create a database, then set `DATABASE_URL`. The API and worker create `job_logs` on startup.

```sql
CREATE DATABASE task_queue;
```

## Setup

```bash
npm install
```

`.env`:

```
REDIS_URL=redis://localhost:6379
DATABASE_URL=postgres://postgres:postgres@localhost:5432/task_queue
PORT=3000
```

## Run

Three processes:

```bash
npm run worker
npm run api
npm run producer
```

Or enqueue via HTTP:

```bash
curl -X POST http://localhost:3000/jobs \
  -H "Content-Type: application/json" \
  -d "{\"taskName\":\"send_welcome_email\",\"data\":{\"email\":\"test@example.com\"}}"

curl http://localhost:3000/jobs/<job-id>
```

Failing task (retries then DLQ):

```bash
curl -X POST http://localhost:3000/jobs \
  -H "Content-Type: application/json" \
  -d "{\"taskName\":\"always_fail\",\"data\":{},\"maxRetries\":2}"
```
