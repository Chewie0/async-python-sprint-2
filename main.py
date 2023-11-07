import logging
import threading
import time
from datetime import datetime, timedelta
from logging import config
from logging_conf import LOG_CONFIG
from scheduler import Scheduler
from job import Job


config.dictConfig(LOG_CONFIG)

def empty_worker():
    print('Я задача!')


def empty_worker2():
    print('Я 2 задача!')
    time.sleep(15)
    print('Я 2 задача конец!')

def run_jobs():
    shed = Scheduler()

    test_job = Job(empty_worker, start_at=datetime.now() + timedelta(seconds=1)) #
    shed.schedule(test_job)


    test_job2 = Job(empty_worker2, max_working_time=3)
    shed.schedule(test_job2)

    shed.start()
    time.sleep(6)
    shed.join()








    '''
    shed.start()
    time.sleep(1)
    test_job2 = Job(empty_worker2)
    shed.schedule(test_job2)#
    time.sleep(1)
    shed.join()
    
    
    
       time.sleep(5)


    test_job2 = Job(empty_worker2, max_working_time=2)
    shed.schedule(test_job2)
    time.sleep(2)
    
    
    
     t = threading.Thread(target=shed.run)
    t.start()
    test_job2 = Job(empty_worker2)
    shed.schedule(test_job2)  #
    shed.stop()
    t.join()
    '''





if __name__ == "__main__":
    run_jobs()