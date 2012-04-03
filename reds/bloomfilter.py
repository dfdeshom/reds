from math import ceil, exp, log, pow
import redis
import random

class RedisBloomFilter(object):
    """
    Bloom filter backed by Redis. Used for storing already-seen urls temporarily
    """
    def __init__(self, connection,capacity, name="urlcache",error_rate=0.001):
        self.k = int(ceil(log(1 / error_rate, 2)))        
        self.n = capacity
        self.m = -1 * (self.n * log(error_rate)) / pow(log(2), 2)
        self.m = int(ceil(self.m))

        self.name = name
        self.connection = connection 

        # keep count in redis
        self.countkey = self.name + ":count"
        self.bits_in_inserted_values = 0
        
    def __contains__(self, key):
        pipeline = self.connection.pipeline()
        for hashed_offset in self.calculate_offsets(key):
            pipeline.getbit(self.name, hashed_offset)
        results = pipeline.execute()
        return all(results)
    
    def add(self, key):
        # if key is present, don't add it
        if key in self:
            return False
        
        # if at capacity, clear the filter for now 
        if self.count >= self.n:
            self.clear()
            return False
        
        pipeline = self.connection.pipeline()
        for hashed_offset in self.calculate_offsets(key):
            pipeline.setbit(self.name, hashed_offset, 1)

        # increase count
        pipeline.incr(self.countkey)
        pipeline.execute()
            
        self.bits_in_inserted_values += 8 * len(key)
        return True
    
    def calculate_offsets(self, key):
        hash1 = FNVHash(key)
        hash2 = APHash(key)
        
        for i in range(self.k):
            yield (hash1 + i * hash2) % self.m

    def clear(self,):
        self.connection.delete(self.name)
        self.connection.delete(self.countkey)
        return
    
    @property
    def count(self):
        r = self.connection.get(self.countkey)
        if r:
            return int(r)
        return 0
    
    def stats(self):
        k = float(self.k)
        m = float(self.m) 
        n = float(self.n)
        p_fp = pow(1.0 - exp(-(k * n) / m), k) * 100.0
        compression_ratio = float(self.bits_in_inserted_values) / m

        s = '\n'
        s += "Space allocated for storage (m) : %d or %s\n" % (m,convert_bytes(float(m)/8.0))
        s += "Capacity (n) : %d elements \n" % (n,)
        s += "Number of elements : %d\n" % self.count
        s += "Number of filter hashes (k) : %d\n" % k
        s += "Predicted false positive rate = %s %%\n" % str(p_fp)
        s += "Compression ratio = %.2f\n" % compression_ratio
        return  s

def FNVHash(key):
    fnv_prime = 0x811C9DC5
    hash = 0
    for i in range(len(key)):
      hash *= fnv_prime
      hash ^= ord(key[i])
    return hash

def APHash(key):
    hash = 0xAAAAAAAA
    for i in range(len(key)):
      if ((i & 1) == 0):
        hash ^= ((hash <<  7) ^ ord(key[i]) * (hash >> 3))
      else:
        hash ^= (~((hash << 11) + ord(key[i]) ^ (hash >> 5)))
    return hash

def convert_bytes(bytes):
    bytes = float(bytes)
    if bytes >= 1099511627776:
        terabytes = bytes / 1099511627776
        size = '%.2fT' % terabytes
    elif bytes >= 1073741824:
        gigabytes = bytes / 1073741824
        size = '%.2fG' % gigabytes
    elif bytes >= 1048576:
        megabytes = bytes / 1048576
        size = '%.2fM' % megabytes
    elif bytes >= 1024:
        kilobytes = bytes / 1024
        size = '%.2fK' % kilobytes
    else:
        size = '%.2fb' % bytes
    return size

