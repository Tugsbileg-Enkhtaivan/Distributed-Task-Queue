import express, { type Request, type Response } from "express";
import dotenv from "dotenv";
import { ensureJobLogsTable } from "./config/postgres";
import { redis } from "./config/redis";
import { enqueueJob } from "./queue/enqueue";
import { getJobLog } from "./queue/jobLogs";
import { jobKey } from "./queue/keys";

dotenv.config();

const PORT = Number.parseInt(process.env.PORT || "3000", 10);

type CreateJobBody = {
    taskName?: string;
    data?: Record<string, unknown>;
    delayMs?: number;
    maxRetries?: number;
};

function hashToJob(jobId: string, hash: Record<string, string>) {
    const rawData = hash["data"];
    let data: unknown = {};
    if (rawData) {
        try {
            data = JSON.parse(rawData);
        } catch {
            data = rawData;
        }
    }

    return {
        id: hash["id"] ?? jobId,
        taskName: hash["taskName"] ?? null,
        status: hash["status"] ?? null,
        attempts: hash["attempts"] ?? null,
        maxRetries: hash["maxRetries"] ?? null,
        data,
        lastError: hash["lastError"] ?? null,
        createdAt: hash["createdAt"] ?? null,
        startedAt: hash["startedAt"] ?? null,
        completedAt: hash["completedAt"] ?? null,
        failedAt: hash["failedAt"] ?? null,
        source: "redis" as const,
    };
}

async function startApi(): Promise<void> {
    await ensureJobLogsTable();

    const app = express();
    app.use(express.json());

    app.post("/jobs", async (request: Request, response: Response) => {
        try {
            const body = (request.body ?? {}) as CreateJobBody;
            if (!body.taskName || typeof body.taskName !== "string") {
                response.status(400).json({ error: "taskName is required" });
                return;
            }

            const enqueueInput: {
                taskName: string;
                data: Record<string, unknown>;
                delayMs?: number;
                maxRetries?: number;
            } = {
                taskName: body.taskName,
                data: body.data ?? {},
            };
            if (typeof body.delayMs === "number") {
                enqueueInput.delayMs = body.delayMs;
            }
            if (typeof body.maxRetries === "number") {
                enqueueInput.maxRetries = body.maxRetries;
            }

            const job = await enqueueJob(enqueueInput);
            response.status(201).json(job);
        } catch (error) {
            console.error("Failed to enqueue job:", error);
            response.status(500).json({ error: "Failed to enqueue job" });
        }
    });

    app.get("/jobs/:id", async (request: Request, response: Response) => {
        try {
            const id = request.params["id"];
            if (typeof id !== "string") {
                response.status(400).json({ error: "id is required" });
                return;
            }

            const hash = await redis.hgetall(jobKey(id));
            if (hash && Object.keys(hash).length > 0) {
                response.json(hashToJob(id, hash));
                return;
            }

            const log = await getJobLog(id);
            if (!log) {
                response.status(404).json({ error: "Job not found" });
                return;
            }

            response.json({
                id: log.id,
                taskName: log.task_name,
                status: log.status,
                attempts: log.attempts,
                data: log.payload,
                lastError: log.error,
                createdAt: log.created_at,
                finishedAt: log.finished_at,
                source: "postgres",
            });
        } catch (error) {
            console.error("Failed to fetch job:", error);
            response.status(500).json({ error: "Failed to fetch job" });
        }
    });

    app.listen(PORT, "0.0.0.0", () => {
        console.log(`API listening on http://localhost:${PORT}`);
    });
}

startApi().catch((error) => {
    console.error("API failed to start:", error);
    process.exit(1);
});
