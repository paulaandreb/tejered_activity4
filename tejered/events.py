"""Patrón Observer distribuido: difusión de eventos por temas."""
from __future__ import annotations

import fnmatch
import queue
import threading
import time
from typing import Any, Callable

from . import protocol


class RemoteSubscriber:
    """Observador remoto con cola propia.

    Cada suscriptor tiene un hilo emisor. Si uno es lento, solo se llena su
    cola y se descartan sus eventos: nunca se bloquea al coordinador.
    """

    def __init__(self, channel: protocol.Channel, topics: list[str], max_queue: int = 10000):
        self.channel = channel
        self.topics = topics
        self.dropped = 0
        self._queue: queue.Queue[dict[str, Any] | None] = queue.Queue(max_queue)
        self.on_failure: Callable[["RemoteSubscriber"], None] | None = None
        self._thread = threading.Thread(target=self._drain, name="subscriber", daemon=True)
        self._thread.start()

    def matches(self, topic: str) -> bool:
        return any(fnmatch.fnmatch(topic, pattern) for pattern in self.topics)

    def notify(self, event: dict[str, Any]) -> None:
        try:
            self._queue.put_nowait(event)
        except queue.Full:
            self.dropped += 1

    def close(self) -> None:
        try:
            self._queue.put_nowait(None)
        except queue.Full:
            pass

    def _drain(self) -> None:
        while True:
            event = self._queue.get()
            if event is None:
                return
            try:
                self.channel.send(event)
            except OSError:
                if self.on_failure:
                    self.on_failure(self)
                return


class EventBus:
    """Sujeto observable: los componentes publican sin conocer a los observadores."""

    def __init__(self) -> None:
        self._subscribers: list[RemoteSubscriber] = []
        self._lock = threading.Lock()

    def subscribe(self, subscriber: RemoteSubscriber) -> None:
        subscriber.on_failure = self.unsubscribe
        with self._lock:
            self._subscribers.append(subscriber)

    def unsubscribe(self, subscriber: RemoteSubscriber) -> None:
        with self._lock:
            if subscriber in self._subscribers:
                self._subscribers.remove(subscriber)

    def publish(self, topic: str, **data: Any) -> None:
        event = {"type": protocol.EVENT, "topic": topic, "ts": time.time(), "data": data}
        with self._lock:
            targets = [s for s in self._subscribers if s.matches(topic)]
        for subscriber in targets:
            subscriber.notify(event)
