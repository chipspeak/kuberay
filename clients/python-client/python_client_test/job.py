import ray
import time

ray.init()

@ray.remote
def hello():
    time.sleep(2)  # Simulate some work
    return "Hello from Ray!"

result = ray.get(hello.remote())
print(result) 