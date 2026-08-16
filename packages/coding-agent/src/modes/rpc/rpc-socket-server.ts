import { lstatSync, type Stats, unlinkSync } from "node:fs";
import { chmod, lstat, unlink } from "node:fs/promises";
import { createConnection, createServer, type Server, type Socket } from "node:net";
import type { AgentSessionRuntime } from "../../core/agent-session-runtime.ts";
import { runRpcConnection } from "./rpc-mode.ts";

export interface RpcSocketServer {
	readonly path: string;
	close(): Promise<void>;
}

async function removeStaleSocket(path: string): Promise<void> {
	let stats: Stats;
	try {
		stats = await lstat(path);
	} catch (error: unknown) {
		if ((error as NodeJS.ErrnoException).code === "ENOENT") return;
		throw error;
	}

	if (!stats.isSocket()) {
		throw new Error(`RPC socket path exists and is not a socket: ${path}`);
	}

	const inUse = await new Promise<boolean>((resolve, reject) => {
		const probe = createConnection(path);
		probe.once("connect", () => {
			probe.destroy();
			resolve(true);
		});
		probe.once("error", (error: NodeJS.ErrnoException) => {
			probe.destroy();
			if (error.code === "ECONNREFUSED" || error.code === "ENOENT") {
				resolve(false);
			} else {
				reject(error);
			}
		});
	});

	if (inUse) {
		throw new Error(`RPC socket is already in use: ${path}`);
	}
	await unlink(path);
}

async function listen(server: Server, path: string): Promise<void> {
	await new Promise<void>((resolve, reject) => {
		const onError = (error: Error) => {
			server.off("listening", onListening);
			reject(error);
		};
		const onListening = () => {
			server.off("error", onError);
			resolve();
		};
		server.once("error", onError);
		server.once("listening", onListening);
		server.listen(path);
	});
}

function createSocketWriter(socket: Socket): {
	write: (line: string) => void;
	waitForOutput: () => Promise<void>;
} {
	let drainPromise: Promise<void> | undefined;
	let resolveDrain: (() => void) | undefined;

	const settleDrain = () => {
		resolveDrain?.();
		resolveDrain = undefined;
		drainPromise = undefined;
	};
	socket.on("drain", settleDrain);
	socket.on("close", settleDrain);

	return {
		write: (line) => {
			if (socket.destroyed || socket.write(line) || drainPromise) return;
			drainPromise = new Promise<void>((resolve) => {
				resolveDrain = resolve;
			});
		},
		waitForOutput: () => drainPromise ?? Promise.resolve(),
	};
}

export async function startRpcSocketServer(runtimeHost: AgentSessionRuntime, path: string): Promise<RpcSocketServer> {
	if (process.platform === "win32") {
		throw new Error("--rpc-socket is only supported on Unix platforms");
	}

	await removeStaleSocket(path);
	const sockets = new Set<Socket>();
	let acceptingConnections = false;
	const server = createServer((socket) => {
		if (!acceptingConnections) {
			socket.destroy();
			return;
		}
		sockets.add(socket);
		socket.once("close", () => sockets.delete(socket));
		const writer = createSocketWriter(socket);
		void runRpcConnection(runtimeHost, {
			input: socket,
			write: writer.write,
			waitForOutput: writer.waitForOutput,
		})
			.catch(() => socket.destroy())
			.finally(() => socket.end());
	});

	await listen(server, path);
	let identity: Stats;
	try {
		await chmod(path, 0o600);
		identity = await lstat(path);
	} catch (error: unknown) {
		await new Promise<void>((resolve) => server.close(() => resolve()));
		await unlink(path).catch(() => {});
		throw error;
	}
	acceptingConnections = true;
	let closed = false;

	const unlinkOwnedSocketSync = () => {
		try {
			const current = lstatSync(path);
			if (current.dev === identity.dev && current.ino === identity.ino) {
				unlinkSync(path);
			}
		} catch {}
	};
	process.once("exit", unlinkOwnedSocketSync);

	return {
		path,
		async close() {
			if (closed) return;
			closed = true;
			process.off("exit", unlinkOwnedSocketSync);
			for (const socket of sockets) socket.destroy();
			await new Promise<void>((resolve, reject) => {
				server.close((error) => (error ? reject(error) : resolve()));
			});
			unlinkOwnedSocketSync();
		},
	};
}
