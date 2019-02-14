"""
An example of how logging can be added to a Dask/Dask Distributed system where
workers are in different threads or processes.
"""

from dask import compute, delayed
from dask.distributed import Client

import logging
import logging.handlers
import multiprocessing

from random import choice, random
import time


LEVELS = [
    logging.DEBUG,
    logging.INFO,
    logging.WARNING,
    logging.ERROR,
    logging.CRITICAL,
]

MESSAGES = [
    'Example message #1',
    'Example message #2',
    'Example message #3',
]


def configure_logging():
    """
    Configures the logger, in this example setting only the format.

    In a production system this could be used to read a log config file and do
    more detailed configuration.
    """
    log_format = '%(asctime)s %(name)s %(levelname)-8s %(message)s'
    logging.basicConfig(format=log_format)


def worker_logging_configurer(queue):
    """
    Configures the logging for a worker process.

    Args:
        queue -- the queue to which messages should be sent.
    """
    handler = logging.handlers.QueueHandler(queue)
    root = logging.getLogger()
    root.addHandler(handler)
    root.setLevel(logging.DEBUG)


@delayed
def worker_task(task_id):
    """
    This is a dummy task for a worker to run, it's simulating doing some work
    and creating some random log messages and random levels.

    Args:
        id -- the id of this task so we can make it clean which is which in the
        logging demo.
    """
    name = f'{task_id}'
    logger = logging.getLogger()
    logger.info('Worker started: %s' % name)
    for i in range(10):
        time.sleep(random())
        level = choice(LEVELS)
        message = choice(MESSAGES)
        logger.log(level, message)
    logger.info('Worker finished: %s' % name)


class LogListener:
    """
    Manages the Listener process and configuration of the message queue.
    """
    def __init__(self, logging_configurer):
        """
        Initialises the Log Listener and the queue and process used.
        """
        self._logging_configurer = logging_configurer
        self._manager = multiprocessing.Manager()
        self._queue = self._manager.Queue(-1)
        self._listener = None

    @property
    def queue(self):
        """
        Exposes the queue which needs to be passed to other processes for
        logging.
        """
        return self._queue

    def start(self):
        """
        Start the Log Listener process
        """
        self._listener = \
            multiprocessing.Process(target=self._listener_process)
        self._listener.start()

    def stop(self):
        """
        Stop the Log Listener process and clean up.
        """
        self._queue.put_nowait(None)
        self._listener.join()

    def _listener_process(self):
        """
        The logging main loop to be executed in the logging process.
        """
        self._logging_configurer()

        while True:
            try:
                record = self._queue.get()

                # This as a sentinel to tell the listener to quit.
                if record is None:
                    break

                logger = logging.getLogger(record.name)
                logger.handle(record)

            except Exception:
                import sys, traceback
                print('Logging error:', file=sys.stderr)
                traceback.print_exc(file=sys.stderr)


def main():
    """
    This function represents the main function of your Dask based system.

    In this example it creates a simple set of workers and launches some
    dummy tasks that just create random log messages.
    """
    # Configure the log listener and launch it in a seperate process
    log_listener = LogListener(configure_logging)
    log_listener.start()

    # Launch some Dask workers
    client = Client(threads_per_worker=1, n_workers=10)

    # Run the log configuration code on each work in the Dask cluster
    client.run(worker_logging_configurer, log_listener.queue)

    # Create some dummmy task to run on the workers
    # This is where your core computation would be in a real system.
    tasks = [worker_task(i) for i in range(10)]

    # Launch the work on the cluster
    compute(tasks)

    # This is the end of the core computation and now comes any cleanup code.

    # Stop the log listener and clean up
    log_listener.stop()


if __name__ == '__main__':
    main()
