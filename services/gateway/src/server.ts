import Fastify from "fastify";
import cors from "@fastify/cors";
import { randomUUID } from "node:crypto";
import { fetch } from "undici";

const app = Fastify({
    logger: {
        level: process.env.LOG_LEVEL ?? "info",
        redact: ["req.headers.authorization"],
    },
    requestIdHeader: "x-request-id",
    genReqId: () => randomUUID(),
});

async function start() {
    await app.register(cors, {
        origin: process.env.FRONTEND_ORIGIN ?? "http://localhost:5173",
    });

    app.get("/health", async () => ({
        status: "ok",
        service: "DisLog-gateway",
    }));

    app.get("/ready", async () => ({
        status: "ready",
    }));

    app.get("/api/v1", async () => ({
        message: "Welcome to DisLog",
        service: "gateway",
        requestId: "See the x-request-id response header",
    }));


    app.post("/api/v1/events", async (request, reply) => {
        const ingestionUrl =
            process.env.INGESTION_URL ?? "http://127.0.0.1:8000";

        try {
            const upstream = await fetch(
                `${ingestionUrl}/v1/events`,
                {
                    method: "POST",
                    headers: {
                        "content-type": "application/json",
                        "x-request-id": request.id,
                    },
                    body: JSON.stringify(request.body),
                    signal: AbortSignal.timeout(3000),
                },
            );

            const responseBody = await upstream.json();

            return reply
                .code(upstream.status)
                .send(responseBody);

        } catch (error) {
            request.log.error(
                { err: error },
                "Ingestion service request failed",
            );

            const isTimeout =
                error instanceof Error &&
                error.name === "TimeoutError";

            return reply.code(isTimeout ? 504 : 502).send({
                error: isTimeout
                    ? "Ingestion service timed out"
                    : "Ingestion service unavailable",
                requestId: request.id,
            });
        }
    });


    const port = Number(process.env.PORT ?? 3000);

    await app.listen({
        port,
        host: "0.0.0.0",
    });
}

start().catch((error: unknown) => {
    app.log.error(error, "Failed to start gateway");
    process.exit(1);
});



