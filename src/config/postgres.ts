import { Pool } from "pg";
import dotenv from "dotenv";

dotenv.config();

const DATABASE_URL =
    process.env.DATABASE_URL || "postgres://postgres:postgres@localhost:5432/task_queue";

export const pg = new Pool({ connectionString: DATABASE_URL });

const CREATE_JOB_LOGS = `
CREATE TABLE IF NOT EXISTS job_logs (
  id UUID PRIMARY KEY,
  task_name TEXT NOT NULL,
  payload JSONB,
  status TEXT NOT NULL,
  attempts INT NOT NULL,
  error TEXT,
  created_at TIMESTAMPTZ,
  finished_at TIMESTAMPTZ DEFAULT now()
);
`;

export async function ensureJobLogsTable(): Promise<void> {
    await pg.query(CREATE_JOB_LOGS);
}
