import logging
import os
from urllib.request import urlopen, Request
import time
from datetime import datetime, timedelta
from logging import config
from logging_conf import LOG_CONFIG
from scheduler import Scheduler
from job import Job

logging.config.dictConfig(LOG_CONFIG)



def empty_worker():
    print('I am an empty task!')


def empty_worker2():
    print('I am the second empty task with pause! Lets begin!')
    time.sleep(10)
    print('I am the second empty task with pause! I finished!')


def working_with_file_system():
    print('working_with_file_system')
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
    #os.remove(path)
    print('end_working_with_file_system')


def working_with_files():
    print('working_with_files')
    with open('testfile', 'w') as f:
        for i in range(1,5):
            f.write(f'I am {str(i)}th string from testfile \n')
    with open('testfile', 'r') as f:
        for line in f.readlines():
            print(line)
    print('end_working_with_files')

def work_with_network():
    url = "https://jsonplaceholder.typicode.com/posts/1"
    httprequest = Request(url, headers={"Accept": "application/json"})
    with urlopen(httprequest) as response:
        print(response.status)
        print(response.read().decode())



def run_jobs():
    shed = Scheduler()

    job_file_syst = Job(working_with_file_system, shed=shed)
    job_work_with_file = Job(working_with_files, shed=shed, dependencies=[job_file_syst.id], tries=3)
    job_work_with_network = Job(work_with_network, shed=shed, dependencies=[job_work_with_file.id] , tries=3)



    job_empty = Job(empty_worker,  shed=shed, start_at=datetime.now() + timedelta(seconds=8))
    job_empty2 = Job(empty_worker2, shed=shed, max_working_time=1) # max_working_time=3

    shed.schedule(job_empty2)
    shed.schedule(job_file_syst)
    shed.schedule(job_work_with_file)
    shed.schedule(job_work_with_network)
    shed.schedule(job_empty)
    shed.schedule(job_empty2)

    shed.start()
    time.sleep(4)

    shed.stop()
    time.sleep(4)
    shed.restart()
    print('restart')
    time.sleep(6)





if __name__ == "__main__":
    run_jobs()
