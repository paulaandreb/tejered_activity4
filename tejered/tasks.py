"""Catálogo de tareas que ejecutan los nodos trabajadores."""
from __future__ import annotations

import hashlib
import time
from typing import Any, Callable


def noop(_: dict[str, Any], __: float) -> dict[str, Any]:
    """No hace trabajo; sirve para medir el costo del propio middleware."""
    return {"ok": True}


def io_sim(payload: dict[str, Any], slowdown: float) -> dict[str, Any]:
    """Simula una operación de E/S, como leer un archivo o consultar un servicio."""
    time.sleep(payload.get("ms", 20) / 1000 * slowdown)
    return {"waited_ms": payload.get("ms", 20)}


def cpu_hash(payload: dict[str, Any], slowdown: float) -> dict[str, Any]:
    """Trabajo de CPU en Python puro: el GIL impide paralelizarlo entre hilos."""
    rounds = int(payload.get("rounds", 20000) * slowdown)
    value = 0
    for number in range(rounds):
        value = (value * 31 + number) % 1_000_003
    return {"value": value}


def checksum(payload: dict[str, Any], slowdown: float) -> dict[str, Any]:
    """Calcula el SHA-256 de un texto, como validación de integridad de un registro."""
    text = str(payload.get("text", "")).encode("utf-8")
    if slowdown > 1:
        time.sleep(0.001 * slowdown)
    return {"sha256": hashlib.sha256(text).hexdigest()}


def word_count(payload: dict[str, Any], slowdown: float) -> dict[str, Any]:
    """Cuenta palabras de un documento."""
    if slowdown > 1:
        time.sleep(0.001 * slowdown)
    return {"words": len(str(payload.get("text", "")).split())}


CATALOG: dict[str, Callable[[dict[str, Any], float], dict[str, Any]]] = {
    "noop": noop,
    "io_sim": io_sim,
    "cpu_hash": cpu_hash,
    "checksum": checksum,
    "word_count": word_count,
}
