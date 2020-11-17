from typing import Tuple, Dict, List, Callable, Any
from multiprocessing import Array, Process, JoinableQueue
from queue import Empty

class ThreadPoolProcess(Process):
    def __init__(self, tasks: JoinableQueue, resources_init: List[Callable[[], Tuple[str, Any]]] = [],
            resources_close: Dict[str, Callable[[Any], None]] = dict()):
        super().__init__()
        self._stopping = False
        self._tasks = tasks
        self._resources: Dict[str, Any] = dict([res() for res in resources_init])
        # self._results = results
        self._resources_close = resources_close
    def run(self) -> None:
        while True:
            try:
                function, task = self._tasks.get(timeout=10)
                if task is None:
                    break
                try:
                    if isinstance(task, list) or isinstance(task, tuple):
                        # self._results.append(self._function(*task, **self._resources))
                        function(*task, **self._resources)
                    else:
                        # self._results.append(self._function(task, **self._resources))
                        function(task, **self._resources)
                finally:
                    self._tasks.task_done()
            except Empty:
                pass
        for resource, close_func in self._resources_close.items():
            if resource in self._resources:
                close_func(self._resources[resource])

class ThreadPool:
    def __init__(self, threads: int, resources_init: List[Callable[[], Tuple[str, Any]]] = [],
            resources_close: Dict[str, Callable[[Any], None]] = dict(), max_size = None):
        self.queue: JoinableQueue
        if max_size is not None:
            self.queue = JoinableQueue(max_size)
        else:
            self.queue = JoinableQueue()
        self._results: List[Any] = list()
        self.threads = [ThreadPoolProcess(self.queue, resources_init, resources_close) for _ in range(threads)]
        for thread in self.threads:
            thread.start()
    def execute(self, function: Callable[..., Any], task: Any):
        self.queue.put((function, task))
    def stop(self):
        for _ in range(len(self.threads)):
            self.queue.put(None)
    def join(self):
        self.queue.join()
        print('Queue joined')
        for thread in self.threads:
            if thread.is_alive:
                print('Thread is alive, putting None')
                self.queue.put(None)
        for thread in self.threads:
            if thread.is_alive:
                thread.join()
    def results(self):
        res = self._results.copy()
        self._results.clear()
        return res