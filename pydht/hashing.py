import hashlib

def hash_function(data):
    return int(hashlib.md5(data).hexdigest(), 16)