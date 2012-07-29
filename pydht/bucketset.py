import heapq
import threading

from .peer import Peer

def largest_differing_bit(value1, value2):
    distance = value1 ^ value2
    length = -1
    while (distance):
        distance >>= 1
        length += 1
    return max(0, length)

class BucketSet(object):
    def __init__(self, bucket_size, buckets, id):
        self.id = id
        self.bucket_size = bucket_size
        self.buckets = [list() for _ in range(buckets)]
        self.lock = threading.Lock()
        
    def insert(self, peer):
        if peer.id != self.id:
            bucket_number = largest_differing_bit(self.id, peer.id)
            peer_triple = peer.astriple()
            with self.lock:
                bucket = self.buckets[bucket_number]
                if peer_triple in bucket: 
                    bucket.pop(bucket.index(peer_triple))
                elif len(bucket) >= self.bucket_size:
                    bucket.pop(0)
                bucket.append(peer_triple)
                
    def nearest_nodes(self, key, limit=None):
        num_results = limit if limit else self.bucket_size
        with self.lock:
            def keyfunction(peer):
                return key ^ peer[2] # ideally there would be a better way with names? Instead of storing triples it would be nice to have a dict
            peers = (peer for bucket in self.buckets for peer in bucket)
            best_peers = heapq.nsmallest(self.bucket_size, peers, keyfunction)
            return [Peer(*peer) for peer in best_peers]
