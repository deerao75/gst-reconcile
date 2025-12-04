import os
import sys
from multiprocessing import Process
from redis import Redis
from rq import Worker, Connection

# Define how many workers to run.
# Start with 4 for your 2GB instance. If memory holds up, try 3 later.
NUM_WORKERS = 4

def start_worker(worker_name):
    redis_url = os.environ.get('REDIS_URL', 'redis://localhost:6379/0')
    conn = Redis.from_url(redis_url)

    # We listen to the 'reconcile' queue
    queues = ['reconcile']

    with Connection(conn):
        # Create a worker with a specific name
        w = Worker(queues, name=worker_name)
        print(f"Starting worker: {worker_name}")
        w.work()

if __name__ == '__main__':
    processes = []
    print(f"Spinning up {NUM_WORKERS} concurrent workers...")

    for i in range(NUM_WORKERS):
        # Name them worker-1, worker-2, etc.
        name = f"worker-{i+1}"
        p = Process(target=start_worker, args=(name,))
        p.start()
        processes.append(p)

    # Wait for all processes (this keeps the script running)
    for p in processes:
        p.join()
