/**
 * Worker threads for the CPU-bound parts of the delays step: reducing one
 * Ist-Daten day to a sample file, one aggregation pass, writing one set of shards.
 *
 * A task first takes one of the CPU slots it shares with the timetable parses,
 * then an idle worker - started on demand, so there are never more threads than
 * tasks the slots let run at once.
 *
 * The thread side lives at the bottom of this file: the same module, loaded as a
 * worker, answers each message with delays.js' runTask().
 */

import { Worker, isMainThread, parentPort } from 'node:worker_threads';

export class WorkerPool {
	constructor(cpu) {
		this.cpu = cpu;
		this.idle = [];
		this.all = new Set();
	}

	/** Runs `task` on a worker; `transfer` lists buffers to move rather than copy. */
	run(task, transfer = []) {
		return this.cpu.run(
			() =>
				new Promise((resolve, reject) => {
					const worker = this.idle.pop() ?? this.spawn();
					const settle = () => {
						worker.off('message', onMessage);
						worker.off('error', onError);
						worker.off('exit', onExit);
					};
					const onMessage = (message) => {
						settle();
						this.idle.push(worker);
						if (message.error) reject(new Error(`${task.kind}: ${message.error}`));
						else resolve(message.result);
					};
					const onError = (error) => {
						settle();
						this.all.delete(worker);
						reject(error);
					};
					const onExit = (code) => {
						settle();
						this.all.delete(worker);
						reject(new Error(`${task.kind}: worker exited with code ${code}`));
					};
					worker.on('message', onMessage);
					worker.on('error', onError);
					worker.on('exit', onExit);
					worker.postMessage(task, transfer);
				})
		);
	}

	spawn() {
		const worker = new Worker(new URL(import.meta.url));
		this.all.add(worker);
		return worker;
	}

	async close() {
		const workers = [...this.all];
		this.all.clear();
		this.idle = [];
		await Promise.all(workers.map((worker) => worker.terminate()));
	}
}

if (!isMainThread) {
	const { runTask } = await import('./delays.js');
	parentPort.on('message', async (task) => {
		try {
			const { result, transfer = [] } = await runTask(task);
			parentPort.postMessage({ result }, transfer);
		} catch (error) {
			parentPort.postMessage({ error: error?.stack ?? String(error) });
		}
	});
}
