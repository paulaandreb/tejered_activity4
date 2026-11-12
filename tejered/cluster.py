"""Lanza nodos como procesos independientes para simular una red en un equipo."""
from __future__ import annotations

import os
import signal
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


@dataclass
class WorkerSpec:
    worker_id: str
    threads: int = 4
    slowdown: float = 1.0


class LocalCluster:
    """Cada nodo es un proceso del sistema operativo unido por TCP."""

    def __init__(self, port: int, heartbeat: float = 0.3, nodelay: bool = True,
                 split_writes: bool = False):
        self.port = port
        self.heartbeat = heartbeat
        self.nodelay = nodelay
        self.split_writes = split_writes
        self.processes: dict[str, subprocess.Popen] = {}

    def add_worker(self, spec: WorkerSpec) -> None:
        command = [
            sys.executable, "-m", "tejered.worker",
            "--id", spec.worker_id, "--port", str(self.port),
            "--threads", str(spec.threads), "--slowdown", str(spec.slowdown),
            "--heartbeat", str(self.heartbeat),
        ]
        if not self.nodelay:
            command.append("--no-nodelay")
        if self.split_writes:
            command.append("--split-writes")
        env = {**os.environ, "PYTHONPATH": str(ROOT)}
        self.processes[spec.worker_id] = subprocess.Popen(command, env=env, cwd=ROOT)

    def kill(self, worker_id: str) -> None:
        """Simula la caída abrupta del proceso."""
        self.processes[worker_id].kill()

    def freeze(self, worker_id: str) -> None:
        """Simula un nodo colgado: la conexión sigue abierta, pero no responde."""
        self.processes[worker_id].send_signal(signal.SIGSTOP)

    def resume(self, worker_id: str) -> None:
        self.processes[worker_id].send_signal(signal.SIGCONT)

    def shutdown(self) -> None:
        for process in self.processes.values():
            try:
                process.send_signal(signal.SIGCONT)
                process.kill()
            except ProcessLookupError:
                pass
        for process in self.processes.values():
            process.wait(timeout=10)
