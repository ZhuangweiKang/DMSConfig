class queue:
    def __init__(self, capacity=10):
        """
        capacity 队列大小
        """
        self.capacity = capacity
        self.reset()

    def reset(self):
        self.size = 0
        self.front = 0
        self.rear = 0
        self.array = [0] * self.capacity

    def is_empty(self):
        # 判断队列是否为空
        return 0 == self.size

    def is_full(self):
        # 判断队列是否已满
        return self.size == self.capacity

    def enqueue(self, element):
        # 入队
        if self.is_full():
            # 判断是否已满
            raise Exception('queue is full')  # 自定义错误信息

        self.array[self.rear] = element
        self.size += 1
        self.rear = (self.rear + 1) % self.capacity

    def dequeue(self):
        if self.is_empty():
            # 判断队列是否为空
            raise Exception('queue is empty')

        self.size -= 1
        self.front = (self.front + 1) % self.capacity  # 更改队首位置

    def get_front(self):
        # 获取队首
        return self.array[self.front]

    def get_array(self):
        return self.array