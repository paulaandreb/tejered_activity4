import socket
import threading
import time
import unittest

from tejered import protocol
from tejered.cluster import LocalCluster, WorkerSpec
from tejered.coordinator import Coordinator, CoordinatorConfig
from tejered.proxy import CoordinatorProxy, EventListener
from tejered.strategies import LeastLoaded, RoundRobin


class FakeWorker:
    def __init__(self, worker_id, capacity, in_flight):
        self.worker_id, self.capacity, self.in_flight = worker_id, capacity, in_flight


class ProtocolTests(unittest.TestCase):
    def test_messages_survive_fragmentation_and_concurrency(self):
        left, right = socket.socketpair()
        sender, receiver = protocol.Channel(left), protocol.Channel(right)
        total = 200

        def send(prefix):
            for number in range(total):
                sender.send({"who": prefix, "n": number, "text": "ñ" * 500})

        threads = [threading.Thread(target=send, args=(p,)) for p in "abc"]
        for thread in threads:
            thread.start()
        received = [receiver.recv() for _ in range(total * 3)]
        for thread in threads:
            thread.join()
        for prefix in "abc":
            numbers = [m["n"] for m in received if m["who"] == prefix]
            self.assertEqual(numbers, list(range(total)))


class StrategyTests(unittest.TestCase):
    def test_round_robin_alternates(self):
        workers = [FakeWorker("a", 4, 9), FakeWorker("b", 4, 0)]
        strategy = RoundRobin()
        self.assertEqual([strategy.select(workers).worker_id for _ in range(4)], list("abab"))

    def test_least_loaded_uses_relative_load(self):
        workers = [FakeWorker("a", 2, 2), FakeWorker("b", 8, 4)]
        self.assertEqual(LeastLoaded().select(workers).worker_id, "b")


class ClusterTests(unittest.TestCase):
    def setUp(self):
        self.coordinator = Coordinator(CoordinatorConfig(heartbeat_timeout=0.8))
        self.port = self.coordinator.start()
        self.cluster = LocalCluster(self.port, heartbeat=0.1)
        self.proxy = CoordinatorProxy("127.0.0.1", self.port)

    def tearDown(self):
        self.cluster.shutdown()
        self.proxy.close()
        self.coordinator.stop()

    def start_workers(self, count):
        for number in range(count):
            self.cluster.add_worker(WorkerSpec(f"w{number}", threads=4))
        self.proxy.wait_for_workers(count)

    def test_all_tasks_complete(self):
        self.start_workers(2)
        job = self.proxy.submit({"kind": "word_count", "payload": {"text": "uno dos"}} for _ in range(100))
        status = self.proxy.wait(job)
        self.assertEqual((status["done"], status["failed"]), (100, 0))

    def test_failing_task_is_reported_after_retries(self):
        self.start_workers(1)
        job = self.proxy.submit([{"kind": "no_existe"}])
        status = self.proxy.wait(job)
        self.assertEqual(status["failed"], 1)
        self.assertEqual(status["attempts"][f"{job}-0"], 3)

    def test_crash_does_not_lose_tasks(self):
        self.start_workers(3)
        job = self.proxy.submit({"kind": "io_sim", "payload": {"ms": 10}} for _ in range(300))
        time.sleep(0.15)
        self.cluster.kill("w0")
        status = self.proxy.wait(job)
        self.assertEqual(status["done"], 300)
        self.assertEqual(self.proxy.stats()["metrics"]["workers_lost"], 1)

    def test_frozen_worker_is_detected_and_late_results_ignored(self):
        self.start_workers(2)
        job = self.proxy.submit({"kind": "io_sim", "payload": {"ms": 20}} for _ in range(200))
        time.sleep(0.1)
        self.cluster.freeze("w0")
        status = self.proxy.wait(job)
        self.assertEqual(status["done"], 200)
        self.cluster.resume("w0")
        time.sleep(0.2)
        self.assertEqual(self.proxy.status(job)["done"], 200)

    def test_observer_receives_node_and_job_events(self):
        received = []
        listener = EventListener("127.0.0.1", self.port, received.append)
        time.sleep(0.05)
        self.start_workers(1)
        job = self.proxy.submit([{"kind": "noop"}])
        self.proxy.wait(job)
        time.sleep(0.1)
        listener.close()
        topics = {event["topic"] for event in received}
        self.assertTrue({"node.up", "job.submitted", "job.finished"} <= topics)


if __name__ == "__main__":
    unittest.main(verbosity=2)
