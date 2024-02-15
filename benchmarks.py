from improv.store import StoreInterface, PlasmaStoreInterface

import numpy as np
import subprocess
import time
import pickle
import cProfile

WAIT_TIMEOUT = 10

MEMORY_LIMIT = 20000000000


def setup_store(server_port_num=6379, set_store_loc="/tmp/store"):
    """Start the server"""
    p = None
    if StoreInterface != PlasmaStoreInterface:
        print("Setting up Redis store.")
        p = subprocess.Popen(
            ["redis-server",
             "--port", str(server_port_num),
             "--maxmemory",
             str(MEMORY_LIMIT),
             "--save",
             '""',
             "--io-threads",
             "4"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )

    else:
        """Start the server"""
        print("Setting up Plasma store.")
        p = subprocess.Popen(
            ["plasma_store", "-s", set_store_loc, "-m", str(MEMORY_LIMIT)],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
    print(f"PID: {p.pid}")
    return p


def test_store(store, message_count, message_size, test_log):
    messages = [np.random.random(message_size) for i in range(message_count)]
    print(f"Storing {message_count} messages of size {message_size} "
          f"for a total of {message_size * message_count * 8}B.")
    if type(store) is not PlasmaStoreInterface:
        start_time = time.perf_counter()
        i = 0
        for message in messages:
            i += 1
            store.put(message)
            if i % 10 == 0:
                store.client.execute()
        end_time = time.perf_counter()
        test_log.append([message_count, message_size, end_time - start_time])
        print(f"Finished storing in average {end_time - start_time} seconds.")
    else:
        start_time = time.perf_counter()
        for message in messages:
            store.put(message, "p")
        end_time = time.perf_counter()
        test_log.append([message_count, message_size, end_time - start_time])
        print(f"Finished storing in {end_time - start_time} seconds.")
    if type(store) is PlasmaStoreInterface:
        store.client.delete(store.client.list().keys())
    else:
        store.client.flushall()
    time.sleep(2)
    return


def main():
    configs = []

    for count_exponent in range(0, 7):
        for size_exponent in range(0, 9):
            count = 10 ** count_exponent
            size = 10 ** size_exponent
            if count * size * 8 > 10000000000:
                continue
            elif size * 8 > 5 * (10 ** 8):
                continue
            configs.append((count, size))

    p = setup_store()

    logs = []
    log_name = ""

    if StoreInterface != PlasmaStoreInterface:
        log_name = "redis_logs.txt"
        store = StoreInterface()
        for count, size in configs:
            test_store(store, count, size, logs)
    else:
        log_name = "plasma_logs.txt"
        store = StoreInterface()
        for count, size in configs:
            test_store(store, count, size, logs)

    print("shutting down store")
    p.kill()
    p.wait(WAIT_TIMEOUT)

    with open(log_name, "wb") as f:
        f.write(pickle.dumps(logs, protocol=5))

    return


if __name__ == "__main__":
    main()
