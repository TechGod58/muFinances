from services.background_jobs import BackgroundJobService, BackoffPolicy, JobStatus
from services.base import ServiceContext


class FakeCursor:
    def __init__(self, rows=None, columns=None):
        self._rows = rows or []
        self.description = [(column,) for column in (columns or [])]

    def fetchall(self):
        return self._rows


class FakeDb:
    def __init__(self):
        self.jobs = {}
        self.logs = []
        self.statements = []

    def execute(self, sql, parameters=()):
        self.statements.append((sql, parameters))
        normalized = " ".join(sql.lower().split())
        if normalized.startswith("insert into background_jobs"):
            self.jobs[parameters[0]] = {
                "job_id": parameters[0],
                "job_type": parameters[1],
                "status": "queued",
                "payload_json": parameters[2],
                "attempts": 0,
            }
        if normalized.startswith("select job_id, job_type, status, attempts, payload_json"):
            for job in self.jobs.values():
                if job["status"] == "queued":
                    return FakeCursor(
                        [(job["job_id"], job["job_type"], job["status"], job["attempts"], job["payload_json"])],
                        ["job_id", "job_type", "status", "attempts", "payload_json"],
                    )
            return FakeCursor([], ["job_id", "job_type", "status", "attempts", "payload_json"])
        if normalized.startswith("update background_jobs set status = 'running'"):
            self.jobs[parameters[3]]["status"] = "running"
            self.jobs[parameters[3]]["attempts"] = parameters[1]
        if normalized.startswith("update background_jobs set status = 'completed'"):
            self.jobs[parameters[2]]["status"] = "completed"
        if normalized.startswith("update background_jobs set status = 'dead_letter'"):
            self.jobs[parameters[2]]["status"] = "dead_letter"
        if normalized.startswith("update background_jobs set status = 'queued'"):
            self.jobs[parameters[2]]["status"] = "queued"
        if normalized.startswith("update background_jobs set status = 'cancelled'"):
            self.jobs[parameters[2]]["status"] = "cancelled"
        if normalized.startswith("insert into background_job_logs"):
            self.logs.append(parameters)
        if normalized.startswith("select status, count(*)"):
            counts = {}
            for job in self.jobs.values():
                counts[job["status"]] = counts.get(job["status"], 0) + 1
            return FakeCursor([(status, count) for status, count in counts.items()], ["status", "count"])
        return FakeCursor()

    def executemany(self, sql, parameters):
        self.statements.append((sql, list(parameters)))

    def commit(self):
        pass

    def rollback(self):
        pass


def test_enqueue_and_lease_job():
    db = FakeDb()
    service = BackgroundJobService(db)
    context = ServiceContext(user_id="admin", roles=("admin",))

    service.enqueue(context, {"job_id": "job-1", "job_type": "report.export", "payload": {"report": "board"}})
    leased = service.lease_next("worker-1")

    assert leased.job_id == "job-1"
    assert leased.status is JobStatus.RUNNING
    assert db.jobs["job-1"]["attempts"] == 1


def test_fail_moves_to_dead_letter_after_max_attempts():
    db = FakeDb()
    service = BackgroundJobService(db, backoff=BackoffPolicy(max_attempts=2))
    context = ServiceContext(user_id="admin", roles=("admin",))
    db.jobs["job-1"] = {"job_id": "job-1", "job_type": "x", "status": "running", "payload_json": "{}", "attempts": 2}

    status = service.fail(context, "job-1", "boom", attempts=2)

    assert status is JobStatus.DEAD_LETTER
    assert db.jobs["job-1"]["status"] == "dead_letter"


def test_health_probe_reports_dead_letter_unhealthy():
    db = FakeDb()
    service = BackgroundJobService(db)
    db.jobs["job-1"] = {"job_id": "job-1", "job_type": "x", "status": "dead_letter", "payload_json": "{}", "attempts": 3}

    health = service.health_probe()

    assert health.dead_letter == 1
    assert health.healthy is False

