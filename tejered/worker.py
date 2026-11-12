"""Nodo trabajador: recibe tareas por red y las ejecuta en un grupo de hilos."""
from __future__ import annotations

import argparse
import queue
import socket
import threading
import time
from typing import Any

from . import protocol
from .tasks import CATALOG


class Worker:
    """Patrón Thread Pool: un número fijo de hilos consume una cola interna.

    Hilos del nodo:
      * principal: lee las asignaciones que llegan por la red.
      * ``exec-N``: ejecutan las tareas.
      * ``heartbeat``: informa periódicamente que el nodo sigue vivo.
    """

    def __init__(
        self,
        worker_id: str,
        host: str,
        port: int,
        threads: int = 4,
        slowdown: float = 1.0,
        heartbeat_interval: float = 0.3,
        nodelay: bool = True,
        split_writes: bool = False,
    ):
        self.worker_id = worker_id
        self.address = (host, port)
        self.threads = threads
        self.slowdown = slowdown
        self.heartbeat_interval = heartbeat_interval
        self.nodelay = nodelay
        self.split_writes = split_writes
        self._queue: queue.Queue[dict[str, Any] | None] = queue.Queue()
        self._stop = threading.Event()
        self._channel: protocol.Channel | None = None

    def run(self) -> None:
        sock = socket.create_connection(self.address)
        protocol.tune_socket(sock, self.nodelay)
        self._channel = protocol.Channel(sock, split_writes=self.split_writes)
        self._channel.send(
            {"type": protocol.REGISTER, "worker_id": self.worker_id, "capacity": self.threads}
        )
        for number in range(self.threads):
            threading.Thread(target=self._execute_loop, name=f"exec-{number}", daemon=True).start()
        threading.Thread(target=self._heartbeat_loop, name="heartbeat", daemon=True).start()
        try:
            while not self._stop.is_set():
                message = self._channel.recv()
                if message["type"] == protocol.ASSIGN:
                    self._queue.put(message)
                elif message["type"] == protocol.SHUTDOWN:
                    break
        except (protocol.ConnectionClosed, ConnectionError, OSError):
            pass
        finally:
            self._stop.set()
            for _ in range(self.threads):
                self._queue.put(None)
            self._channel.close()

    def _execute_loop(self) -> None:
        assert self._channel is not None
        while True:
            message = self._queue.get()
            if message is None:
                return
            reply: dict[str, Any] = {
                "type": protocol.RESULT,
                "worker_id": self.worker_id,
                "task_id": message["task_id"],
                "attempt": message["attempt"],
            }
            try:
                handler = CATALOG[message["kind"]]
                reply["output"] = handler(message["payload"], self.slowdown)
                reply["ok"] = True
            except Exception as error:  # una tarea defectuosa no debe tumbar el nodo
                reply["ok"] = False
                reply["error"] = f"{type(error).__name__}: {error}"
            try:
                self._channel.send(reply)
            except OSError:
                return

    def _heartbeat_loop(self) -> None:
        assert self._channel is not None
        while not self._stop.wait(self.heartbeat_interval):
            try:
                self._channel.send({"type": protocol.HEARTBEAT, "worker_id": self.worker_id})
            except OSError:
                return


def main() -> None:
    parser = argparse.ArgumentParser(description="Nodo trabajador de TejeRed")
    parser.add_argument("--id", required=True)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=9400)
    parser.add_argument("--threads", type=int, default=4)
    parser.add_argument("--slowdown", type=float, default=1.0)
    parser.add_argument("--heartbeat", type=float, default=0.3)
    parser.add_argument("--no-nodelay", action="store_true")
    parser.add_argument("--split-writes", action="store_true")
    args = parser.parse_args()
    # Una vez conectado, el proceso solo termina por SHUTDOWN o por pérdida de conexión.
    Worker(
        args.id,
        args.host,
        args.port,
        args.threads,
        args.slowdown,
        args.heartbeat,
        not args.no_nodelay,
        args.split_writes,
    ).run()
    time.sleep(0.05)


if __name__ == "__main__":
    main()
