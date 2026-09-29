class Topic:
    def __init__(self, name):
        self.name = name
        self.subscribers = []
    def subscribe(self, callback):
        self.subscribers.append(callback)
    def get_subscribers(self):
        return self.subscribers