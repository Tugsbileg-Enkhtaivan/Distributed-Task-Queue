import { randomUUID } from "crypto";
import { redis } from "../config/redis";
import { DELAYED_QUEUE, WAITING_QUEUE, jobKey } from "./keys";

export type EnqueueInput = {
    taskName: string;
    data: Record<string, unknown>;
    delayMs?: number;
    maxRetries?: number;
};

export type EnqueuedJob = {
    id: string;
    status: "waiting";
};

export async function enqueueJob(input: EnqueueInput): Promise<EnqueuedJob> {
    const jobId = randomUUID();
    const delayMs = input.delayMs ?? 0;
    const maxRetries = input.maxRetries ?? 3;
    const createdAt = Date.now().toString();
    const metaKey = jobKey(jobId);

    const jobPayload: Record<string, string> = {
        id: jobId,
        taskName: input.taskName,
        data: JSON.stringify(input.data),
        status: "waiting",
        attempts: "0",
        maxRetries: String(maxRetries),
        createdAt,
    };

    const pipeline = redis.pipeline();
    pipeline.hset(metaKey, jobPayload);

    if (delayMs > 0) {
        pipeline.zadd(DELAYED_QUEUE, Date.now() + delayMs, jobId);
    } else {
        pipeline.lpush(WAITING_QUEUE, jobId);
    }

    await pipeline.exec();
    return { id: jobId, status: "waiting" };
}
