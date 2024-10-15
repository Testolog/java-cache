from collections import deque



class LRUCache:
    def __init__(self, capacity:int) -> None:
        self.capacity = capacity
        self.q = deque(maxlen=capacity)
        self.cache = {}

    def put(self, key:int, value:int)->None:
        if len(self.cache) == self.capacity:
            last = self.q.pop()
            del self.cache[last]
        self.q.appendleft(key)
        self.cache[key] = value



    def get(self, key:int)->int | None:
        if key not in self.cache:
            return None
        self.q.remove(key)
        self.q.appendleft(key)
        return self.cache[key];


if __name__ == '__main__':
    cache = LRUCache(2)
    cache.put(1,1)
    cache.put(2,2)
    cache.put(3,3)
    assert cache.get(1) is None
    cache = LRUCache(2)
    cache.put(1,1)
    cache.put(2,2)
    assert cache.get(1) == 1
    cache.put(3,3)
    assert cache.get(2) is None
    assert cache.get(1) == 1 
