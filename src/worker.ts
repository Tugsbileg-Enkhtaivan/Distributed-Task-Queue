import { randomUUID } from "crypto";
import { redis } from "./config/redis";
import { ensureJobLogsTable } from "./config/postgres";
import { persistJobLog } from "./queue/jobLogs";
import { WAITING_QUEUE, ACTIVE_QUEUE, heartbeatKey, jobKey } from "./queue/keys";
import { claimJob, finalizeClaim, promoteDelayedJobs, reclaimOrphanedJobs } from "./queue/scripts";
import { markCompleted, markFailed, scheduleRetry } from "./queue/retry";

const CONCURRENCY = 3;
const HEARTBEAT_TTL_SECONDS = 5;
let activeJobsCount = 0;
const workerId = randomUUID();

const taskHandlers: Record<string, (data: Record<string, unknown>) => Promise<void>> = {
    send_welcome_email: async (data) => {
        console.log(`[Task] Sending welcome email to: ${String(data["email"])}...`);
        await new Promise((resolve) => setTimeout(resolve, 2000));
        console.log(`[Task] Email sent successfully to: ${String(data["email"])}`);
    },
    always_fail: async () => {
        throw new Error("Forced failure for retry/DLQ demo");
    },
};

function parseJobInt(value: string | undefined, fallback: number): number {
    const parsed = Number.parseInt(value ?? "", 10);
    return Number.isFinite(parsed) ? parsed : fallback;
}

function parsePayload(raw: string | undefined): unknown {
    if (!raw) {
        return {};
    }
    try {
        return JSON.parse(raw);
    } catch {
        return {};
    }
}

async function processJob(jobId: string): Promise<void> {
    const metaKey = jobKey(jobId);

    try {
        const jobData = await redis.hgetall(metaKey);
        if (!jobData || Object.keys(jobData).length === 0) {
            console.warn(`Job metadata not found for ID: [${jobId}]`);
            await redis.lrem(ACTIVE_QUEUE, 1, jobId);
            return;
        }

        const taskName = jobData["taskName"];
        if (!taskName) {
            throw new Error("Job is missing taskName");
        }

        console.log(`Processing job [${jobId}] -> Task: "${taskName}"`);

        const handler = taskHandlers[taskName];
        if (!handler) {
            throw new Error(`No handler registered for task type: ${taskName}`);
        }

        const parsedPayload = parsePayload(jobData["data"]);
        await handler((parsedPayload ?? {}) as Record<string, unknown>);

        await markCompleted(redis, jobId);
        await persistJobLog({
            id: jobId,
            taskName,
            payload: parsedPayload,
            status: "completed",
            attempts: parseJobInt(jobData["attempts"], 0),
            createdAtMs: parseJobInt(jobData["createdAt"], Date.now()),
        });
        console.log(`Job [${jobId}] completed successfully.`);
    } catch (error) {
        const message = error instanceof Error ? error.message : String(error);
        console.error(`Job [${jobId}] failed with error:`, message);

        const jobData = await redis.hgetall(metaKey);
        const currentAttempts = parseJobInt(jobData["attempts"], 0) + 1;
        const maxRetries = parseJobInt(jobData["maxRetries"], 3);
        const taskName = jobData["taskName"] ?? "unknown";
        const payload = parsePayload(jobData["data"]);

        if (currentAttempts <= maxRetries) {
            const delay = await scheduleRetry(redis, jobId, currentAttempts, message);
            console.log(
                `Re-queuing job [${jobId}] with ${delay}ms backoff (Attempt ${currentAttempts}/${maxRetries})`,
            );
            return;
        }

        await markFailed(redis, jobId, message);
        await persistJobLog({
            id: jobId,
            taskName,
            payload,
            status: "failed",
            attempts: currentAttempts,
            error: message,
            createdAtMs: parseJobInt(jobData["createdAt"], Date.now()),
        });
        console.error(`Job [${jobId}] reached max retries. Moved to dead-letter log.`);
    }
}

async function startWorker(): Promise<void> {
    await ensureJobLogsTable();
    const blockingClient = redis.duplicate();

    const heartbeatTimer = setInterval(() => {
        void redis.set(heartbeatKey(workerId), Date.now().toString(), "EX", HEARTBEAT_TTL_SECONDS);
    }, 2000);

    const maintenanceTimer = setInterval(() => {
        void (async () => {
            const promoted = await promoteDelayedJobs(redis);
            if (promoted > 0) {
                console.log(`Promoted ${promoted} delayed job(s) to waiting`);
            }
            const reclaimed = await reclaimOrphanedJobs(redis);
            if (reclaimed > 0) {
                console.log(`Reclaimed ${reclaimed} orphaned job(s)`);
            }
        })();
    }, 1000);

    await redis.set(heartbeatKey(workerId), Date.now().toString(), "EX", HEARTBEAT_TTL_SECONDS);
    console.log(
        `Worker ${workerId} initialized. Listening on "${WAITING_QUEUE}" (Concurrency: ${CONCURRENCY})`,
    );

    const shutdown = async () => {
        clearInterval(heartbeatTimer);
        clearInterval(maintenanceTimer);
        blockingClient.disconnect();
        redis.disconnect();
        process.exit(0);
    };

    process.on("SIGINT", () => {
        void shutdown();
    });
    process.on("SIGTERM", () => {
        void shutdown();
    });

    while (true) {
        try {
            if (activeJobsCount >= CONCURRENCY) {
                await new Promise((resolve) => setTimeout(resolve, 100));
                continue;
            }

            let jobId = await claimJob(redis, workerId);

            if (!jobId) {
                const moved = await blockingClient.blmove(
                    WAITING_QUEUE,
                    ACTIVE_QUEUE,
                    "RIGHT",
                    "LEFT",
                    5,
                );
                if (moved) {
                    jobId = await finalizeClaim(redis, workerId, moved);
                }
            }

            if (!jobId) {
                continue;
            }

            activeJobsCount += 1;
            void processJob(jobId).finally(() => {
                activeJobsCount -= 1;
            });
        } catch (error) {
            console.error("Error in worker polling loop:", error);
            await new Promise((resolve) => setTimeout(resolve, 1000));
        }
    }
}

startWorker().catch((error) => {
    console.error("Worker failed to start:", error);
    process.exit(1);
});
