import logging
import os
from urllib.request import urlopen, Request
import time
from datetime import datetime, timedelta

from logging_conf import LOG_CONFIG
from scheduler import Scheduler
from job import Job


logging.config.dictConfig(LOG_CONFIG)
logger = logging.getLogger()


def bad_job():
    return 1/0


def empty_worker():
    print('I am an empty task!')


def empty_worker2():
    print('I am a second empty task with pause! Lets begin!')
    time.sleep(3)
    print('I am a second empty task with pause! I finished!')


def working_with_file_system():
    logger.info('working_with_file_system')
    current_dir = os.path.abspath(os.curdir)
    path = os.path.join(current_dir, "test_folder")
    filedir = os.path.join(path, 'file')
    movedfile = os.path.join(current_dir, 'movedfile')
    isExist = os.path.exists(path)
    if not isExist:
        os.makedirs(path)
    with open(filedir, 'w+') as f:
        f.write('test')
    os.replace(filedir, movedfile)
    os.remove(movedfile)
    time.sleep(2)
    logger.info('end_working_with_file_system')


def working_with_files():
    logger.info('working_with_files')
    with open('testfile', 'w') as f:
        for i in range(1, 5):
            f.write(f'I am {str(i)}th string from testfile \n')
    with open('testfile', 'r') as f:
        for line in f.readlines():
            print(line)
    logger.info('end_working_with_files')


def work_with_network():
    logger.info('work_with_network')
    url = "https://jsonplaceholder.typicode.com/posts/1"
    httprequest = Request(url, headers={"Accept": "application/json"})
    with urlopen(httprequest) as response:
        print(response.status)
        print(response.read().decode())
    logger.info('end_work_with_network')


def run_jobs():
    shed = Scheduler()

    job_file_syst = Job(working_with_file_system, )
    job_work_with_file = Job(working_with_files, dependencies=[job_file_syst.id], tries=3)
    job_work_with_network = Job(work_with_network, dependencies=[job_work_with_file.id], tries=3)

    job_empty = Job(empty_worker, start_at=datetime.now() + timedelta(seconds=8))
    job_empty2 = Job(empty_worker2, max_working_time=5)
    job_bad_job = Job(bad_job, tries=3)

    shed.schedule(job_bad_job)
    shed.schedule(job_empty2)
    shed.schedule(job_file_syst)
    shed.schedule(job_work_with_file)
    shed.schedule(job_work_with_network)
    shed.schedule(job_empty)

    shed.start()
    time.sleep(5)
    shed.stop()

    time.sleep(10)
    shed.restart()
    time.sleep(10)
    shed.join()


if __name__ == "__main__":
    run_jobs()
