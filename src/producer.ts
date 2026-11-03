import { enqueueJob } from "./queue/enqueue";

async function run() {
    const job = await enqueueJob({
        taskName: "send_welcome_email",
        data: { userId: 9942, email: "test@example.com" },
    });
    console.log(`Enqueued job [${job.id}] for task: "send_welcome_email"`);
    process.exit(0);
}

run().catch((error) => {
    console.error("Producer failed:", error);
    process.exit(1);
});
