import threading

from .peer import Peer

class Shortlist(object):
    def __init__(self, k, key):
        self.k = k
        self.key = key
        self.list = list()
        self.lock = threading.Lock()
        self.completion_value = None
        
    def set_complete(self, value):
        with self.lock:
            self.completion_value = value
            
    def completion_result(self):
        with self.lock:
            return self.completion_value
        
    def update(self, nodes):
        for node in nodes:
            self._update_one(node)
        
    def _update_one(self, node):
        if node.id == self.key or self.completion_value:
            return
        with self.lock:
            for i in range(len(self.list)):
                if node.id == self.list[i][0][2]:
                    break
                if node.id ^ self.key < self.list[i][0][2] ^ self.key:
                    self.list.insert(i, (node.astriple(), False))
                    self.list = self.list[:self.k]
                    break
            else:
                if len(self.list) < self.k:
                    self.list.append((node.astriple(), False))
                    
    def mark(self, node):
        with self.lock:
            for i in range(len(self.list)):
                if node.id == self.list[i][0][2]:
                    self.list[i] = (node.astriple(), True)
                    
    def complete(self):
        if self.completion_value:
            return True
        with self.lock:
            for node, completed in self.list:
                if not completed:
                    return False
            return True
            
    def get_next_iteration(self, alpha):
        if self.completion_value:
            return []
        next_iteration = []
        with self.lock:
            for node, completed in self.list:
                if not completed:
                    next_iteration.append(Peer(*node))
                    if len(next_iteration) >= alpha:
                        break
        return next_iteration
        
    def results(self):
        with self.lock:
            return [Peer(*node) for (node, completed) in self.list]