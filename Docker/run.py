#!/usr/bin/python
#import time
#import os
#starttime=time.time()
#while True:
#    print('Running covid19.py')
#    os.system('python covid19.py')
#    print('Finished running covid19.py')
#    time.sleep(1800 - ((time.time() - starttime) % 1800))
from crontab import CronTab

cron = CronTab(user=False)
job = cron.new(command='python covid19.py', comment='Covid19 Script')
job.hour.every(4)
job.enable
job.every_reboot()
cron.write()