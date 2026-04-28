from __future__ import annotations

import argparse
import os
import time

from app import db
from app.services.foundation import ensure_foundation_ready
from app.services.security import ensure_security_ready, user_profile
from app.services.performance_reliability import promote_due_jobs, run_next_job


def main() -> None:
    parser = argparse.ArgumentParser(description='muFinances durable background worker')
    parser.add_argument('--worker-id', default=os.getenv('CAMPUS_FPM_WORKER_ID', 'mufinances-worker'))
    parser.add_argument('--interval', type=float, default=float(os.getenv('CAMPUS_FPM_WORKER_INTERVAL', '5')))
    parser.add_argument('--once', action='store_true')
    parser.add_argument('--user-id', type=int, default=int(os.getenv('CAMPUS_FPM_WORKER_USER_ID', '1')))
    args = parser.parse_args()

    db.init_db()
    ensure_foundation_ready()
    ensure_security_ready()
    user = user_profile(args.user_id)

    while True:
        promote_due_jobs()
        run_next_job(user, worker_id=args.worker_id)
        if args.once:
            break
        time.sleep(args.interval)


if __name__ == '__main__':
    main()
