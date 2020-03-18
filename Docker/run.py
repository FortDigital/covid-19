#!/usr/bin/python
import time
import os
starttime=time.time()
while True:
    os.system('python covid19.py')
    time.sleep(1800 - ((time.time() - starttime) % 1800))