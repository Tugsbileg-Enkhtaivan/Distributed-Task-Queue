import { pg } from "../config/postgres";

export type JobLogStatus = "completed" | "failed";

export type JobLogRow = {
    id: string;
    task_name: string;
    payload: unknown;
    status: string;
    attempts: number;
    error: string | null;
    created_at: Date | null;
    finished_at: Date | null;
};

export async function persistJobLog(input: {
    id: string;
    taskName: string;
    payload: unknown;
    status: JobLogStatus;
    attempts: number;
    error?: string;
    createdAtMs?: number;
}): Promise<void> {
    const createdAt = input.createdAtMs ? new Date(input.createdAtMs) : null;
    await pg.query(
        `INSERT INTO job_logs (id, task_name, payload, status, attempts, error, created_at, finished_at)
         VALUES ($1, $2, $3::jsonb, $4, $5, $6, $7, now())
         ON CONFLICT (id) DO UPDATE SET
           status = EXCLUDED.status,
           attempts = EXCLUDED.attempts,
           error = EXCLUDED.error,
           finished_at = EXCLUDED.finished_at`,
        [
            input.id,
            input.taskName,
            JSON.stringify(input.payload ?? null),
            input.status,
            input.attempts,
            input.error ?? null,
            createdAt,
        ],
    );
}

export async function getJobLog(id: string): Promise<JobLogRow | null> {
    const result = await pg.query<JobLogRow>("SELECT * FROM job_logs WHERE id = $1", [id]);
    return result.rows[0] ?? null;
}
