"""Patrón Mediator: el coordinador centraliza la interacción entre nodos.

Los trabajadores y los clientes nunca se comunican entre sí; todos hablan con
el coordinador, que además mantiene el estado de cada tarea.

Hilos del coordinador:
  * ``acceptor``: acepta conexiones nuevas.
  * ``conn-*``: uno por conexión, lee los mensajes de ese extremo.
  * ``dispatcher``: asigna las tareas pendientes a los nodos.
  * ``monitor``: detecta nodos caídos por ausencia de latidos.
"""
from __future__ import annotations

import argparse
import itertools
import socket
import threading
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Any

from . import protocol
from .events import EventBus, RemoteSubscriber
from .strategies import build_strategy


@dataclass
class Task:
    task_id: str
    job_id: str
    kind: str
    payload: dict[str, Any]
    attempts: int = 0
    worker_id: str | None = None
    created_at: float = field(default_factory=time.time)
    finished_at: float | None = None
    state: str = "PENDING"
    output: Any = None
    error: str | None = None


@dataclass
class WorkerState:
    worker_id: str
    capacity: int
    channel: protocol.Channel
    last_seen: float
    in_flight: int = 0
    completed: int = 0
    tasks: set[str] = field(default_factory=set)


@dataclass
class Job:
    job_id: str
    total: int
    done: int = 0
    failed: int = 0
    created_at: float = field(default_factory=time.time)
    finished_at: float | None = None
    task_ids: list[str] = field(default_factory=list)
    finished: threading.Event = field(default_factory=threading.Event)


@dataclass(frozen=True)
class CoordinatorConfig:
    host: str = "127.0.0.1"
    port: int = 0
    strategy: str = "least_loaded"
    window: float = 1.0  # tareas en vuelo permitidas por hilo; 0 significa sin límite
    heartbeat_timeout: float = 1.5
    monitor_interval: float = 0.1
    max_attempts: int = 3
    nodelay: bool = True
    split_writes: bool = False
    publish_task_events: bool = False


class Coordinator:
    def __init__(self, config: CoordinatorConfig | None = None):
        self.config = config or CoordinatorConfig()
        self.strategy = build_strategy(self.config.strategy)
        self.events = EventBus()
        self._state = threading.Condition()
        self._pending: deque[str] = deque()
        self._tasks: dict[str, Task] = {}
        self._jobs: dict[str, Job] = {}
        self._workers: dict[str, WorkerState] = {}
        self._ids = itertools.count(1)
        self._stop = threading.Event()
        self.metrics = {
            "reassigned": 0,
            "duplicates_ignored": 0,
            "workers_lost": 0,
            "failed": 0,
            "completed": 0,
        }
        self._server: socket.socket | None = None
        self.port: int | None = None

    # --- Ciclo de vida -----------------------------------------------------
    def start(self) -> int:
        self._server = socket.create_server((self.config.host, self.config.port), backlog=256)
        self.port = self._server.getsockname()[1]
        for target, name in (
            (self._accept_loop, "acceptor"),
            (self._dispatch_loop, "dispatcher"),
            (self._monitor_loop, "monitor"),
        ):
            threading.Thread(target=target, name=name, daemon=True).start()
        return self.port

    def stop(self) -> None:
        self._stop.set()
        with self._state:
            self._state.notify_all()
            workers = list(self._workers.values())
        for worker in workers:
            try:
                worker.channel.send({"type": protocol.SHUTDOWN})
            except OSError:
                pass
        if self._server:
            self._server.close()

    # --- Conexiones --------------------------------------------------------
    def _accept_loop(self) -> None:
        assert self._server is not None
        while not self._stop.is_set():
            try:
                sock, _ = self._server.accept()
            except OSError:
                return
            protocol.tune_socket(sock, self.config.nodelay)
            channel = protocol.Channel(sock, split_writes=self.config.split_writes)
            threading.Thread(
                target=self._serve_connection, args=(channel,), name="conn", daemon=True
            ).start()

    def _serve_connection(self, channel: protocol.Channel) -> None:
        worker_id: str | None = None
        subscriber: RemoteSubscriber | None = None
        try:
            while not self._stop.is_set():
                message = channel.recv()
                kind = message["type"]
                if kind == protocol.REGISTER:
                    worker_id = message["worker_id"]
                    self._register_worker(worker_id, message["capacity"], channel)
                elif kind == protocol.HEARTBEAT:
                    self._touch(message["worker_id"])
                elif kind == protocol.RESULT:
                    self._handle_result(message)
                elif kind == protocol.SUBMIT:
                    self._handle_submit(channel, message)
                elif kind == protocol.JOB_STATUS:
                    channel.send(self._job_status(message["job_id"], message.get("detail", False)))
                elif kind == protocol.STATS:
                    channel.send(self.snapshot())
                elif kind == protocol.SUBSCRIBE:
                    subscriber = RemoteSubscriber(channel, message.get("topics", ["*"]))
                    self.events.subscribe(subscriber)
        except (protocol.ConnectionClosed, ConnectionError, OSError, ValueError):
            pass
        finally:
            if worker_id is not None:
                self._lose_worker(worker_id, channel, reason="conexion_cerrada")
            if subscriber is not None:
                self.events.unsubscribe(subscriber)
                subscriber.close()
            channel.close()

    # --- Registro y salud de nodos -----------------------------------------
    def _register_worker(self, worker_id: str, capacity: int, channel: protocol.Channel) -> None:
        with self._state:
            self._workers[worker_id] = WorkerState(worker_id, capacity, channel, time.monotonic())
            self._state.notify_all()
        channel.send({"type": protocol.REGISTERED, "worker_id": worker_id})
        self.events.publish("node.up", worker_id=worker_id, capacity=capacity)

    def _touch(self, worker_id: str) -> None:
        with self._state:
            worker = self._workers.get(worker_id)
            if worker:
                worker.last_seen = time.monotonic()

    def _monitor_loop(self) -> None:
        while not self._stop.wait(self.config.monitor_interval):
            limit = time.monotonic() - self.config.heartbeat_timeout
            with self._state:
                silent = [w for w in self._workers.values() if w.last_seen < limit]
            for worker in silent:
                self._lose_worker(worker.worker_id, worker.channel, reason="sin_latidos")

    def _lose_worker(self, worker_id: str, channel: protocol.Channel, reason: str) -> None:
        """Retira un nodo y devuelve sus tareas a la cola."""
        with self._state:
            worker = self._workers.get(worker_id)
            if worker is None or worker.channel is not channel:
                return
            del self._workers[worker_id]
            self.metrics["workers_lost"] += 1
            recovered = 0
            abandoned: list[Task] = []
            for task_id in worker.tasks:
                task = self._tasks[task_id]
                if task.state != "RUNNING":
                    continue
                task.worker_id = None
                if task.attempts >= self.config.max_attempts:
                    task.error = f"Agotó {task.attempts} intentos"
                    self._finish(task, "FAILED")
                    abandoned.append(task)
                else:
                    task.state = "PENDING"
                    self._pending.appendleft(task.task_id)
                    self.metrics["reassigned"] += 1
                    recovered += 1
            self._state.notify_all()
        channel.close()
        self.events.publish("node.down", worker_id=worker_id, reason=reason, recovered=recovered)

    # --- Trabajos y resultados ---------------------------------------------
    def _handle_submit(self, channel: protocol.Channel, message: dict[str, Any]) -> None:
        job_id = f"J{next(self._ids)}"
        job = Job(job_id, len(message["tasks"]))
        with self._state:
            self._jobs[job_id] = job
            for index, spec in enumerate(message["tasks"]):
                task_id = f"{job_id}-{index}"
                self._tasks[task_id] = Task(task_id, job_id, spec["kind"], spec.get("payload", {}))
                job.task_ids.append(task_id)
                self._pending.append(task_id)
            if job.total == 0:
                job.finished_at = time.time()
                job.finished.set()
            self._state.notify_all()
        channel.send({"type": protocol.SUBMITTED, "job_id": job_id, "total": job.total})
        self.events.publish("job.submitted", job_id=job_id, total=job.total)

    def _handle_result(self, message: dict[str, Any]) -> None:
        publish_task = None
        job_done = None
        with self._state:
            worker = self._workers.get(message["worker_id"])
            task = self._tasks.get(message["task_id"])
            if worker and message["task_id"] in worker.tasks:
                worker.tasks.discard(message["task_id"])
                worker.in_flight -= 1
                worker.completed += 1
            is_current = (
                task is not None
                and task.state == "RUNNING"
                and task.worker_id == message["worker_id"]
                and task.attempts == message["attempt"]
            )
            if not is_current:
                # Resultado tardío de un nodo que se dio por perdido: la tarea ya
                # se reasignó. Ignorarlo mantiene la exactitud del conteo.
                self.metrics["duplicates_ignored"] += 1
                self._state.notify_all()
                return
            if message["ok"]:
                task.output = message.get("output")
                self._finish(task, "DONE")
                publish_task = task
            elif task.attempts >= self.config.max_attempts:
                task.error = message.get("error")
                self._finish(task, "FAILED")
                publish_task = task
            else:
                task.state = "PENDING"
                task.worker_id = None
                self._pending.append(task.task_id)
                self.metrics["reassigned"] += 1
            job = self._jobs[task.job_id]
            if job.finished.is_set():
                job_done = job
            self._state.notify_all()
        if publish_task is not None and self.config.publish_task_events:
            self.events.publish(
                "task." + publish_task.state.lower(),
                task_id=publish_task.task_id,
                worker_id=message["worker_id"],
            )
        if job_done is not None:
            self.events.publish(
                "job.finished", job_id=job_done.job_id, done=job_done.done, failed=job_done.failed
            )

    def _finish(self, task: Task, state: str) -> None:
        """Debe llamarse con el candado de estado tomado."""
        task.state = state
        task.finished_at = time.time()
        job = self._jobs[task.job_id]
        if state == "DONE":
            job.done += 1
            self.metrics["completed"] += 1
        else:
            job.failed += 1
            self.metrics["failed"] += 1
        if job.done + job.failed == job.total:
            job.finished_at = time.time()
            job.finished.set()

    # --- Despacho ----------------------------------------------------------
    def _limit(self, worker: WorkerState) -> float:
        if self.config.window <= 0:
            return float("inf")
        return max(1, int(worker.capacity * self.config.window))

    def _dispatch_loop(self) -> None:
        while not self._stop.is_set():
            with self._state:
                while not self._stop.is_set():
                    available = [
                        w for w in self._workers.values() if w.in_flight < self._limit(w)
                    ]
                    if self._pending and available:
                        break
                    self._state.wait(0.2)
                if self._stop.is_set():
                    return
                worker = self.strategy.select(available)
                if worker is None:
                    continue
                task = self._tasks[self._pending.popleft()]
                task.state = "RUNNING"
                task.worker_id = worker.worker_id
                task.attempts += 1
                worker.in_flight += 1
                worker.tasks.add(task.task_id)
                message = {
                    "type": protocol.ASSIGN,
                    "task_id": task.task_id,
                    "kind": task.kind,
                    "payload": task.payload,
                    "attempt": task.attempts,
                }
                channel = worker.channel
                worker_id = worker.worker_id
            try:
                channel.send(message)
            except OSError:
                self._lose_worker(worker_id, channel, reason="error_de_envio")

    # --- Consultas ---------------------------------------------------------
    def _job_status(self, job_id: str, detail: bool) -> dict[str, Any]:
        with self._state:
            job = self._jobs.get(job_id)
            if job is None:
                return {"type": protocol.STATUS_REPLY, "error": "Trabajo desconocido"}
            reply: dict[str, Any] = {
                "type": protocol.STATUS_REPLY,
                "job_id": job.job_id,
                "total": job.total,
                "done": job.done,
                "failed": job.failed,
                "finished": job.finished.is_set(),
                "elapsed": (job.finished_at or time.time()) - job.created_at,
            }
            if detail:
                reply["latencies"] = [
                    self._tasks[t].finished_at - self._tasks[t].created_at
                    for t in job.task_ids
                    if self._tasks[t].finished_at is not None
                ]
                reply["attempts"] = {t: self._tasks[t].attempts for t in job.task_ids}
                reply["states"] = {t: self._tasks[t].state for t in job.task_ids}
            return reply

    def snapshot(self) -> dict[str, Any]:
        with self._state:
            return {
                "type": protocol.STATS_REPLY,
                "strategy": self.strategy.name,
                "pending": len(self._pending),
                "workers": {
                    w.worker_id: {
                        "capacity": w.capacity,
                        "in_flight": w.in_flight,
                        "completed": w.completed,
                    }
                    for w in self._workers.values()
                },
                "metrics": dict(self.metrics),
            }


def main() -> None:
    parser = argparse.ArgumentParser(description="Coordinador de TejeRed")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=9400)
    parser.add_argument("--strategy", default="least_loaded", choices=["least_loaded", "round_robin"])
    parser.add_argument("--window", type=float, default=1.0)
    parser.add_argument("--heartbeat-timeout", type=float, default=1.5)
    parser.add_argument("--no-nodelay", action="store_true")
    parser.add_argument("--split-writes", action="store_true")
    parser.add_argument("--publish-task-events", action="store_true")
    args = parser.parse_args()
    coordinator = Coordinator(
        CoordinatorConfig(
            args.host,
            args.port,
            args.strategy,
            args.window,
            args.heartbeat_timeout,
            nodelay=not args.no_nodelay,
            split_writes=args.split_writes,
            publish_task_events=args.publish_task_events,
        )
    )
    port = coordinator.start()
    print(f"[coordinador] escuchando en {args.host}:{port} con {args.strategy}", flush=True)
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        coordinator.stop()


if __name__ == "__main__":
    main()
