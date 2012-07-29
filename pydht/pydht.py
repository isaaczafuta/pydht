import hashlib
import heapq
import json
import random
import socket
import SocketServer
import threading
import time

k = 20
alpha = 3
id_bits = 128
iteration_sleep = 1

def largest_differing_bit(value1, value2):
    distance = value1 ^ value2
    length = -1
    while (distance):
        distance >>= 1
        length += 1
    return max(0, length)


class DHTRequestHandler(SocketServer.BaseRequestHandler):
    def handle(self):
        try:
            message = json.loads(self.request[0].strip())
            message_type = message["message_type"]
            if message_type == "ping":
                self.handle_ping(message)
            elif message_type == "pong":
                self.handle_pong(message)
            elif message_type == "find_node":
                self.handle_find(message)
            elif message_type == "find_value":
                self.handle_find(message, find_value=True)
            elif message_type == "found_nodes":
                self.handle_found_nodes(message)
            elif message_type == "found_value":
                self.handle_found_value(message)
            elif message_type == "store":
                self.handle_store(message)
        except KeyError, ValueError:
            pass
        peer = Peer(message["source"], self.server.dht.peer)
        self.server.dht.buckets.insert(peer)

            
    def handle_ping(self, message):
        peer = Peer(message["source"], self.server.dht.peer)
        peer.pong()
        
    def handle_pong(self, message):
        pass
        
    def handle_find(self, message, find_value=False):
        id = message["id"]
        peer = Peer(message["source"], self.server.dht.peer)
        if find_value and (id in self.server.dht.data):
            value = self.server.dht.data[id]
            peer.found_value(id, value, message["rpc_id"])
        else:
            nearest_nodes = self.server.dht.buckets.nearest_nodes(id)
            peer.found_nodes(id, nearest_nodes, message["rpc_id"])

    def handle_found_nodes(self, message):
        rpc_id = message["rpc_id"]
        shortlist = self.server.dht.rpc_ids[rpc_id]
        del self.server.dht.rpc_ids[rpc_id]
        shortlist.update(message["nearest_nodes"])
        
    def handle_found_value(self, message):
        rpc_id = message["rpc_id"]
        shortlist = self.server.dht.rpc_ids[rpc_id]
        del self.server.dht.rpc_ids[rpc_id]
        shortlist.set_complete(message["value"])
        
    def handle_store(self, message):
        key = message["id"]
        self.server.dht.data[key] = message["value"]


class DHTServer(SocketServer.ThreadingMixIn, SocketServer.UDPServer):
    pass


class Peer(object):
    ''' DHT Peer Information'''
    def __init__(self, peer, local_peer=None):
        if isinstance(peer, (tuple, list)):
            self.host, self.port = peer
        elif isinstance(peer, Peer):
            self.host, self.port = peer.host, peer.port
        else:
            raise TypeError('need tuple, list, or Peer')
        self.id = int(hashlib.md5(repr(self)).hexdigest(), 16)
        self.local_peer = local_peer

    def astuple(self):
        return (self.host, self.port)
        
    def __repr__(self):
        return repr(self.astuple())

    def _sendmessage(self, message):
        if self.local_peer:
            message["source"] = self.local_peer.astuple()
        encoded = json.dumps(message)
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.sendto(encoded, (self.host, self.port))
        s.close()
        
    def ping(self):
        message = {
            "message_type": "ping"
        }
        self._sendmessage(message)
        
    def pong(self):
        message = {
           "message_type": "pong"
        }
        self._sendmessage(message)
        
    def store(self, key, value):
        message = {
            "message_type": "store",
            "id": key,
            "value": value
        }
        self._sendmessage(message)
        
    def find_node(self, id, rpc_id):
        message = {
            "message_type": "find_node",
            "id": id,
            "rpc_id": rpc_id
        }
        self._sendmessage(message)
        
    def found_nodes(self, id, nearest_nodes, rpc_id):
        message = {
            "message_type": "found_nodes",
            "id": id,
            "nearest_nodes": nearest_nodes,
            "rpc_id": rpc_id
        }
        self._sendmessage(message)
        
    def find_value(self, id, rpc_id):
        message = {
            "message_type": "find_value",
            "id": id,
            "rpc_id": rpc_id
        }
        self._sendmessage(message)
        
    def found_value(self, id, value, rpc_id):
        message = {
            "message_type": "found_value",
            "id": id,
            "value": value,
            "rpc_id": rpc_id
        }
        self._sendmessage(message)

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
            self._update_one(Peer(node))
        
    def _update_one(self, node):
        if node.id == self.key or self.completion_value:
            return
        with self.lock:
            for i in range(len(self.list)):
                if node.id == Peer(self.list[i][0]).id:
                    break
                if node.id ^ self.key < Peer(self.list[i][0]).id ^ self.key:
                    self.list.insert(i, (node.astuple(), False))
                    self.list = self.list[:self.k]
                    break
            else:
                if len(self.list) < self.k:
                    self.list.append((node.astuple(), False))
                    
    def mark(self, node):
        with self.lock:
            for i in range(len(self.list)):
                if Peer(node).id == Peer(self.list[i][0]).id:
                    self.list[i] = (node.astuple(), True)
                    
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
                    next_iteration.append(Peer(node))
                    if len(next_iteration) >= alpha:
                        break
        return next_iteration
        
    def results(self):
        with self.lock:
            return [node for (node, completed) in self.list]

            
class KBuckets(object):
    def __init__(self, k, buckets, id):
        self.id = id
        self.k = k
        self.buckets = [list() for _ in range(buckets)]
        self.lock = threading.Lock()
        
    def insert(self, peer):
        if peer.id != self.id:
            bucket_number = largest_differing_bit(self.id, peer.id)
            peer_tuple = peer.astuple()
            with self.lock:
                bucket = self.buckets[bucket_number]
                if peer_tuple in bucket: 
                    bucket.pop(bucket.index(peer_tuple))
                elif len(bucket) >= self.k:
                    bucket.pop(0)
                bucket.append(peer_tuple)
                
    def nearest_nodes(self, key, limit=None):
        num_results = limit if limit else self.k
        with self.lock:
            def keyfunction(peer):
                return key ^ Peer(peer).id
            allpeers = (item for bucket in self.buckets for item in bucket)
            results = heapq.nsmallest(k, allpeers, keyfunction)
            return results


class DHT(object):
    def __init__(self, local_peer, bootstrap_node=None):
        self.peer = Peer(local_peer)
        self.data = {}
        self.buckets = KBuckets(k, id_bits, self.peer.id)
        self.rpc_ids = {} # should probably have a lock for this
        self.server = DHTServer(self.peer.astuple(), DHTRequestHandler)
        self.server.dht = self
        self.server_thread = threading.Thread(target=self.server.serve_forever)
        self.server_thread.daemon = True
        self.server_thread.start()
        if bootstrap_node:
            self.bootstrap(Peer(bootstrap_node, self.peer))
    
    def iterative_find_nodes(self, key):
        shortlist = Shortlist(k, key)
        shortlist.update(self.buckets.nearest_nodes(key, limit=alpha))
        while not shortlist.complete():
            nearest_nodes = shortlist.get_next_iteration(alpha)
            for peer in nearest_nodes:
                peer = Peer(peer, self.peer)
                shortlist.mark(peer)
                rpc_id = random.getrandbits(id_bits)
                peer.find_node(key, rpc_id)
                self.rpc_ids[rpc_id] = shortlist
            time.sleep(iteration_sleep)
        return shortlist.results()
        
    def iterative_find_value(self, key):
        shortlist = Shortlist(k, key)
        shortlist.update(self.buckets.nearest_nodes(key, limit=alpha))
        while not shortlist.complete():
            nearest_nodes = shortlist.get_next_iteration(alpha)
            for peer in nearest_nodes:
                peer = Peer(peer, self.peer)
                shortlist.mark(peer)
                rpc_id = random.getrandbits(id_bits)
                peer.find_value(key, rpc_id)
                self.rpc_ids[rpc_id] = shortlist
            time.sleep(iteration_sleep)
        return shortlist.completion_result()
            
    def bootstrap(self, bootstrap_node):
        self.buckets.insert(bootstrap_node)
        results = self.iterative_find_nodes(self.peer.id)
                    
    def __getitem__(self, key):
        hashed_key = int(hashlib.md5(repr(key)).hexdigest(), 16)
        result = self.iterative_find_value(hashed_key)
        if result:
            return result
        raise KeyError
        
    def __setitem__(self, key, value):
        hashed_key = int(hashlib.md5(repr(key)).hexdigest(), 16)
        nearest_nodes = self.iterative_find_nodes(hashed_key)
        for node in nearest_nodes:
            Peer(node, self.peer).store(hashed_key, value)
        
    def tick():
        pass
