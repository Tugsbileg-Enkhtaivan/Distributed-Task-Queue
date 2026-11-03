import type { Redis } from "ioredis";
import { ACTIVE_QUEUE, DELAYED_QUEUE, jobKey } from "./keys";

export function backoffMs(attempts: number): number {
    return 2 ** attempts * 1000;
}

export async function scheduleRetry(
    redis: Redis,
    jobId: string,
    attempts: number,
    errorMessage: string,
): Promise<number> {
    const delay = backoffMs(attempts);
    const runAt = Date.now() + delay;
    const metaKey = jobKey(jobId);

    const pipeline = redis.pipeline();
    pipeline.hset(metaKey, {
        status: "waiting",
        attempts: String(attempts),
        lastError: errorMessage,
    });
    pipeline.hdel(metaKey, "workerId", "startedAt");
    pipeline.lrem(ACTIVE_QUEUE, 1, jobId);
    pipeline.zadd(DELAYED_QUEUE, runAt, jobId);
    await pipeline.exec();
    return delay;
}

export async function markFailed(redis: Redis, jobId: string, errorMessage: string): Promise<void> {
    const metaKey = jobKey(jobId);
    const pipeline = redis.pipeline();
    pipeline.hset(metaKey, {
        status: "failed",
        lastError: errorMessage,
        failedAt: Date.now().toString(),
    });
    pipeline.lrem(ACTIVE_QUEUE, 1, jobId);
    await pipeline.exec();
}

export async function markCompleted(redis: Redis, jobId: string): Promise<void> {
    const metaKey = jobKey(jobId);
    const pipeline = redis.pipeline();
    pipeline.hset(metaKey, {
        status: "completed",
        completedAt: Date.now().toString(),
    });
    pipeline.lrem(ACTIVE_QUEUE, 1, jobId);
    await pipeline.exec();
}
