"""Patrón Strategy: política intercambiable para elegir el nodo destino."""
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Protocol, Sequence


class WorkerView(Protocol):
    """Lo mínimo que una estrategia necesita saber de un nodo."""

    worker_id: str
    capacity: int
    in_flight: int


class BalancingStrategy(ABC):
    name = "base"

    @abstractmethod
    def select(self, workers: Sequence[WorkerView]) -> WorkerView | None:
        """Devuelve el nodo elegido, o ``None`` si ninguno admite más trabajo."""


class RoundRobin(BalancingStrategy):
    """Reparte por turnos sin observar la carga. Fue la política de la versión 1."""

    name = "round_robin"

    def __init__(self) -> None:
        self._turn = 0

    def select(self, workers: Sequence[WorkerView]) -> WorkerView | None:
        if not workers:
            return None
        ordered = sorted(workers, key=lambda worker: worker.worker_id)
        chosen = ordered[self._turn % len(ordered)]
        self._turn += 1
        return chosen


class LeastLoaded(BalancingStrategy):
    """Elige el nodo con menor ocupación relativa a su capacidad."""

    name = "least_loaded"

    def select(self, workers: Sequence[WorkerView]) -> WorkerView | None:
        if not workers:
            return None
        return min(workers, key=lambda w: (w.in_flight / w.capacity, w.worker_id))


STRATEGIES = {RoundRobin.name: RoundRobin, LeastLoaded.name: LeastLoaded}


def build_strategy(name: str) -> BalancingStrategy:
    try:
        return STRATEGIES[name]()
    except KeyError as error:
        raise ValueError(f"Estrategia desconocida: {name}") from error
