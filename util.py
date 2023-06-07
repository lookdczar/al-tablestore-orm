import hashlib

def hash_id(id):
    return hashlib.md5(str(id).encode('utf-8')).hexdigest()[:4]