import { Redis } from "ioredis";
import dotenv from "dotenv";

dotenv.config();

const REDIS_URL = process.env.REDIS_URL || "redis://localhost:6379";

export const createRedisClient = (): Redis => {
    const client = new Redis(REDIS_URL, {
        maxRetriesPerRequest: null,
        enableReadyCheck: false,
    });

    client.on("connect", () => {
        console.log("Connected to Redis");
    });

    client.on("error", (error) => {
        console.error("Redis error:", error);
    });

    return client;
};

export const redis = createRedisClient();
