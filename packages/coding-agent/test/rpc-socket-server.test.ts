import { existsSync, mkdtempSync, rmSync, statSync } from "node:fs";
import { createConnection, type Socket } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import type { AgentSessionRuntime } from "../src/core/agent-session-runtime.ts";
import { startRpcSocketServer } from "../src/modes/rpc/rpc-socket-server.ts";

function createSession(sessionId: string) {
	return {
		model: undefined,
		thinkingLevel: "off",
		isStreaming: false,
		isCompacting: false,
		steeringMode: "one-at-a-time",
		followUpMode: "one-at-a-time",
		sessionFile: undefined,
		sessionId,
		sessionName: undefined,
		autoCompactionEnabled: true,
		messages: [],
		pendingMessageCount: 0,
		bindExtensions: vi.fn(),
		subscribe: vi.fn(() => () => {}),
	};
}

function createRuntimeHost() {
	let rebindListener: (() => Promise<void>) | undefined;
	const runtime = {
		session: createSession("first"),
		subscribeRebindSession: vi.fn((listener: () => Promise<void>) => {
			rebindListener = listener;
			return () => {
				rebindListener = undefined;
			};
		}),
	} as unknown as AgentSessionRuntime;

	return {
		runtime,
		async replaceSession(sessionId: string) {
			(runtime as unknown as { session: ReturnType<typeof createSession> }).session = createSession(sessionId);
			await rebindListener?.();
		},
	};
}

async function connect(path: string): Promise<Socket> {
	return new Promise((resolve, reject) => {
		const socket = createConnection(path);
		socket.once("connect", () => resolve(socket));
		socket.once("error", reject);
	});
}

function createClient(socket: Socket) {
	let buffer = "";
	const pending = new Map<string, (value: Record<string, unknown>) => void>();
	socket.setEncoding("utf8");
	socket.on("data", (chunk) => {
		buffer += chunk;
		while (true) {
			const newline = buffer.indexOf("\n");
			if (newline === -1) return;
			const value = JSON.parse(buffer.slice(0, newline)) as Record<string, unknown>;
			buffer = buffer.slice(newline + 1);
			if (typeof value.id === "string") pending.get(value.id)?.(value);
		}
	});

	return {
		request(id: string, command: Record<string, unknown>): Promise<Record<string, unknown>> {
			return new Promise((resolve) => {
				pending.set(id, resolve);
				socket.write(`${JSON.stringify({ id, ...command })}\n`);
			});
		},
	};
}

describe.skipIf(process.platform === "win32")("RPC Unix socket server", () => {
	const cleanups: Array<() => Promise<void> | void> = [];

	afterEach(async () => {
		while (cleanups.length > 0) await cleanups.pop()?.();
	});

	it("serves the active session without binding extension UI and follows replacements", async () => {
		const directory = mkdtempSync(join(tmpdir(), "pi-rpc-socket-"));
		const socketPath = join(directory, "agent.sock");
		const { runtime, replaceSession } = createRuntimeHost();
		const server = await startRpcSocketServer(runtime, socketPath);
		cleanups.push(() => rmSync(directory, { recursive: true, force: true }));
		cleanups.push(() => server.close());

		expect(statSync(socketPath).mode & 0o777).toBe(0o600);
		const socket = await connect(socketPath);
		cleanups.push(() => {
			socket.destroy();
		});
		const client = createClient(socket);

		const first = await client.request("one", { type: "get_state" });
		expect(first).toMatchObject({
			success: true,
			data: { sessionId: "first" },
		});
		expect(runtime.session.bindExtensions).not.toHaveBeenCalled();

		await replaceSession("second");
		const second = await client.request("two", { type: "get_state" });
		expect(second).toMatchObject({
			success: true,
			data: { sessionId: "second" },
		});
		expect(runtime.session.bindExtensions).not.toHaveBeenCalled();
	});

	it("refuses to replace an active socket and removes its path on close", async () => {
		const directory = mkdtempSync(join(tmpdir(), "pi-rpc-socket-"));
		const socketPath = join(directory, "agent.sock");
		const { runtime } = createRuntimeHost();
		const server = await startRpcSocketServer(runtime, socketPath);
		cleanups.push(() => rmSync(directory, { recursive: true, force: true }));
		cleanups.push(() => server.close());

		await expect(startRpcSocketServer(runtime, socketPath)).rejects.toThrow("RPC socket is already in use");
		await server.close();
		expect(existsSync(socketPath)).toBe(false);
	});
});
