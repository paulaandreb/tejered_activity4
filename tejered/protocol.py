"""Protocolo de red: mensajes JSON enmarcados con un prefijo de 4 bytes.

TCP entrega un flujo de bytes, no mensajes. El prefijo de longitud permite
reconstruir cada mensaje sin depender de cómo se fragmenten los paquetes.
"""
from __future__ import annotations

import json
import socket
import struct
import threading
from typing import Any

HEADER = struct.Struct("!I")
MAX_FRAME = 16 * 1024 * 1024

# Tipos de mensaje
REGISTER = "REGISTER"
REGISTERED = "REGISTERED"
HEARTBEAT = "HEARTBEAT"
ASSIGN = "ASSIGN"
RESULT = "RESULT"
SHUTDOWN = "SHUTDOWN"
SUBMIT = "SUBMIT"
SUBMITTED = "SUBMITTED"
SUBSCRIBE = "SUBSCRIBE"
EVENT = "EVENT"
JOB_STATUS = "JOB_STATUS"
STATUS_REPLY = "STATUS_REPLY"
STATS = "STATS"
STATS_REPLY = "STATS_REPLY"


class ConnectionClosed(Exception):
    """El extremo remoto cerró la conexión."""


def tune_socket(sock: socket.socket, nodelay: bool = True) -> None:
    """Aplica las opciones de socket. ``nodelay`` desactiva el algoritmo de Nagle."""
    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1 if nodelay else 0)


def _recv_exact(sock: socket.socket, size: int) -> bytes:
    chunks: list[bytes] = []
    remaining = size
    while remaining:
        chunk = sock.recv(min(remaining, 65536))
        if not chunk:
            raise ConnectionClosed()
        chunks.append(chunk)
        remaining -= len(chunk)
    return b"".join(chunks)


def recv_message(sock: socket.socket) -> dict[str, Any]:
    """Lee un mensaje completo; bloquea hasta recibirlo."""
    (length,) = HEADER.unpack(_recv_exact(sock, HEADER.size))
    if length > MAX_FRAME:
        raise ValueError(f"Trama de {length} bytes supera el máximo permitido")
    return json.loads(_recv_exact(sock, length).decode("utf-8"))


class Channel:
    """Socket con escritura segura entre hilos.

    Varios hilos pueden enviar por el mismo socket; el candado evita que dos
    mensajes se intercalen. ``split_writes`` reproduce la versión inicial del
    prototipo, que enviaba cabecera y cuerpo en dos llamadas separadas.
    """

    def __init__(self, sock: socket.socket, split_writes: bool = False):
        self.sock = sock
        self.split_writes = split_writes
        self._lock = threading.Lock()
        self._closed = False

    def send(self, message: dict[str, Any]) -> None:
        body = json.dumps(message, separators=(",", ":")).encode("utf-8")
        header = HEADER.pack(len(body))
        with self._lock:
            if self.split_writes:
                self.sock.sendall(header)
                self.sock.sendall(body)
            else:
                self.sock.sendall(header + body)

    def recv(self) -> dict[str, Any]:
        return recv_message(self.sock)

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        try:
            self.sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        self.sock.close()
