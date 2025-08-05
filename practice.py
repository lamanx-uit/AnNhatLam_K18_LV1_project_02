import threading
import queue

def producer(q, items):
    for item in items:
        q.put(item)
        print(f"Produced: {item}")
    q.put(None)

def consumer(q):
    while True:
        data = q.get()
        if data is None:
            break
        print(data)
        pass

# TODO: 
# 1. Tạo queue
test = queue.Queue()
items = [1, 2, 3, 4, 5]
# 2. Tạo producer thread với items = [1, 2, 3, 4, 5]
t1 = threading.Thread(target=producer, args=(test, items))
# 3. Tạo consumer thread
t2 = threading.Thread(target=consumer, args=(test,))
# 4. Chạy cả 2 threads
t1.start()
t1.join()
t2.start()
t2.join()