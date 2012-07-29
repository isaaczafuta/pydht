import hashlib
import random

id_bits = 128

def hash_function(data):
    return int(hashlib.md5(data).hexdigest(), 16)
    
def random_id(seed=None):
    if seed:
        random.seed(seed)
    return random.randint(0, (2 ** id_bits)-1)
    