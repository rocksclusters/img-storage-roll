#!/opt/rocks/bin/python

from multiprocessing.pool import ThreadPool
import time
class Test():

    def Func(self):

        def make_sync():
            raise Exception("aaa")

        pool = ThreadPool(processes=1)
        res = pool.apply_async(make_sync)

        print res.get()

        while(not res.ready()):
            time.sleep(1)

        print res.get()

        print "Done"

Test().Func()
