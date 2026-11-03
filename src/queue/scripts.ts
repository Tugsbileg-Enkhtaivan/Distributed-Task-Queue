import { readFileSync } from "fs";
import { dirname, join } from "path";
import { fileURLToPath } from "url";
import type { Redis } from "ioredis";
import {
    ACTIVE_QUEUE,
    DELAYED_QUEUE,
    JOB_PREFIX,
    WAITING_QUEUE,
    WORKER_PREFIX,
    HEARTBEAT_SUFFIX,
} from "./keys";

const luaDir = join(dirname(fileURLToPath(import.meta.url)), "lua");

const CLAIM_LUA = readFileSync(join(luaDir, "claim.lua"), "utf8");
const FINALIZE_LUA = readFileSync(join(luaDir, "finalize.lua"), "utf8");
const PROMOTE_LUA = readFileSync(join(luaDir, "promote.lua"), "utf8");
const RECLAIM_LUA = readFileSync(join(luaDir, "reclaim.lua"), "utf8");

function asJobId(result: unknown): string | null {
    if (typeof result === "string" && result.length > 0) {
        return result;
    }
    return null;
}

export async function claimJob(redis: Redis, workerId: string): Promise<string | null> {
    const result = await redis.eval(
        CLAIM_LUA,
        2,
        WAITING_QUEUE,
        ACTIVE_QUEUE,
        Date.now().toString(),
        workerId,
        JOB_PREFIX,
    );
    return asJobId(result);
}

export async function finalizeClaim(
    redis: Redis,
    workerId: string,
    jobId: string,
): Promise<string | null> {
    const result = await redis.eval(
        FINALIZE_LUA,
        1,
        ACTIVE_QUEUE,
        Date.now().toString(),
        workerId,
        JOB_PREFIX,
        jobId,
    );
    return asJobId(result);
}

export async function promoteDelayedJobs(redis: Redis): Promise<number> {
    const result = await redis.eval(
        PROMOTE_LUA,
        2,
        DELAYED_QUEUE,
        WAITING_QUEUE,
        Date.now().toString(),
        JOB_PREFIX,
    );
    return typeof result === "number" ? result : Number(result ?? 0);
}

export async function reclaimOrphanedJobs(redis: Redis): Promise<number> {
    const result = await redis.eval(
        RECLAIM_LUA,
        2,
        ACTIVE_QUEUE,
        WAITING_QUEUE,
        JOB_PREFIX,
        WORKER_PREFIX,
        HEARTBEAT_SUFFIX,
    );
    return typeof result === "number" ? result : Number(result ?? 0);
}
