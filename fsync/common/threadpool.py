import queue as Queue
import threading
import time

class WorkManager:
    def __init__(self, thread_num=2):
        self.work_queue = Queue.Queue()
        self.threads = []
        self.__init_thread_pool(thread_num)

    def __init_thread_pool(self,thread_num):
        """ 初始化线程 """
        for _ in range(thread_num):
            self.threads.append(Work(self.work_queue))

    def add_job(self, job):
        """ 添加一项工作入队 """
        self.work_queue.put(job) #任务入队，Queue内部实现了同步机制

    def wait_allcomplete(self):
        """ 等待所有线程运行完毕 """
        for item in self.threads:
            item.start()
        for item in self.threads:
            if item.isAlive():item.join()

class Work(threading.Thread):
    def __init__(self, work_queue):
        threading.Thread.__init__(self)
        self.work_queue = work_queue

    def run(self):
        #死循环，从而让创建的线程在一定条件下关闭退出
        while True:
            try:
                job = self.work_queue.get(block=False) #任务异步出队，Queue内部实现了同步机制
                job.execute()
                self.work_queue.task_done() #通知系统任务完成
            except:
                break



if __name__ == '__main__':

    class do_job:
        def __init__(self, num):
            self.num = num
            pass
    
        def execute(self):
            time.sleep(0.1) #模拟处理时间
            print(threading.current_thread(), self.num)

    start = time.time()
    work_manager =  WorkManager(10)

    for i in range(100):
        work_manager.add_job(do_job(i))

    work_manager.wait_allcomplete()
    end = time.time()
    print("cost all time: %s" % (end-start))
