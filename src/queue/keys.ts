export const WAITING_QUEUE = "queue:waiting";
export const DELAYED_QUEUE = "queue:delayed";
export const ACTIVE_QUEUE = "queue:active";
export const JOB_PREFIX = "job:";
export const WORKER_PREFIX = "worker:";
export const HEARTBEAT_SUFFIX = ":heartbeat";

export function jobKey(jobId: string): string {
    return `${JOB_PREFIX}${jobId}`;
}

export function heartbeatKey(workerId: string): string {
    return `${WORKER_PREFIX}${workerId}${HEARTBEAT_SUFFIX}`;
}
