import time
import unittest
import random

from scheduler import Scheduler
from job import Job


class SheduleTest(unittest.TestCase):

    def test_bad_job(self):
        s = Scheduler()
        job = Job(lambda: 1 / 0)
        s.schedule(job)
        s.start()
        time.sleep(1)
        self.assertEqual(job.status, job.Job_stat.FAILED, msg='not failed')
        self.assertEqual(job.tries, 0, msg=' not 0')
        self.assertIsNone(job.result, msg='result not none')
        s.join()

    def test_normal_job(self):
        s = Scheduler()
        job = Job(lambda: 1)
        s.schedule(job)
        s.start()
        time.sleep(1)
        self.assertEqual(job.status, job.Job_stat.DONE)
        self.assertEqual(job.tries, 0)
        self.assertEqual(len(s.running_jobs), 0)
        s.join()

    def test_jobs_with_deps(self):
        s = Scheduler()
        job_1 = Job(lambda: 'hello ')
        job_2 = Job(lambda: 'world', dependencies=[job_1.id])
        job_3 = Job(lambda: '!', dependencies=[job_2.id])
        jobs = [job_1, job_2, job_3]
        random.shuffle(jobs)
        s.schedule(job_1)
        s.schedule(job_2)
        s.schedule(job_3)
        self.assertEqual(len(s.pending_jobs), 3)
        s.start()
        time.sleep(5)
        self.assertEqual("".join([j.result for j in s.done_jobs]), 'hello world!')
        s.join()


if __name__ == '__main__':
    unittest.main()