"""Patrón Proxy: representante local del coordinador remoto.

El código cliente invoca métodos corrientes de Python; el proxy se encarga de
los sockets, la serialización, los reintentos de conexión y la espera de la
respuesta.
"""
from __future__ import annotations

import socket
import threading
import time
from typing import Any, Callable, Iterable

from . import protocol


class CoordinatorProxy:
    def __init__(self, host: str, port: int, retries: int = 20, backoff: float = 0.1):
        self.address = (host, port)
        self.retries = retries
        self.backoff = backoff
        self._channel = self._connect()
        self._lock = threading.Lock()

    def _connect(self) -> protocol.Channel:
        last_error: OSError | None = None
        for attempt in range(self.retries):
            try:
                sock = socket.create_connection(self.address, timeout=5)
                sock.settimeout(None)
                protocol.tune_socket(sock)
                return protocol.Channel(sock)
            except OSError as error:
                last_error = error
                time.sleep(self.backoff * (attempt + 1))
        raise ConnectionError(f"No se pudo contactar al coordinador: {last_error}")

    def _call(self, message: dict[str, Any]) -> dict[str, Any]:
        with self._lock:
            self._channel.send(message)
            return self._channel.recv()

    def submit(self, tasks: Iterable[dict[str, Any]]) -> str:
        reply = self._call({"type": protocol.SUBMIT, "tasks": list(tasks)})
        return reply["job_id"]

    def status(self, job_id: str, detail: bool = False) -> dict[str, Any]:
        return self._call({"type": protocol.JOB_STATUS, "job_id": job_id, "detail": detail})

    def stats(self) -> dict[str, Any]:
        return self._call({"type": protocol.STATS})

    def wait(self, job_id: str, timeout: float = 120, poll: float = 0.02) -> dict[str, Any]:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            status = self.status(job_id)
            if status.get("finished"):
                return self.status(job_id, detail=True)
            time.sleep(poll)
        raise TimeoutError(f"El trabajo {job_id} no terminó en {timeout} s")

    def wait_for_workers(self, count: int, timeout: float = 15) -> None:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if len(self.stats()["workers"]) >= count:
                return
            time.sleep(0.02)
        raise TimeoutError(f"No se registraron {count} nodos a tiempo")

    def close(self) -> None:
        self._channel.close()


class EventListener:
    """Observador remoto: recibe eventos en un hilo y llama a una función."""

    def __init__(self, host: str, port: int, callback: Callable[[dict[str, Any]], None],
                 topics: list[str] | None = None):
        sock = socket.create_connection((host, port))
        protocol.tune_socket(sock)
        self._channel = protocol.Channel(sock)
        self._callback = callback
        self._channel.send({"type": protocol.SUBSCRIBE, "topics": topics or ["*"]})
        self._thread = threading.Thread(target=self._listen, name="listener", daemon=True)
        self._thread.start()

    def _listen(self) -> None:
        try:
            while True:
                self._callback(self._channel.recv())
        except (protocol.ConnectionClosed, ConnectionError, OSError):
            pass

    def close(self) -> None:
        self._channel.close()
