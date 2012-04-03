import redis
from hashlib import sha1

class RedisHashLayer(object):
    """
    A more memory-efficient way to store many small values in redis using hashes.
    See http://antirez.com/post/redis-weekly-update-7.html
    Note: add these config value to redis:

    hash-max-zipmap-entries 512
    hash-max-zipmap-value 512

    """
    def __init__(self,connection,name):
        self.connection = connection
        self.name = name

    def _get_hashname(self,key):
        field = sha1(str(key)).hexdigest()
        hashkey = "%s:%s" % (self.name, field[:4])
        return (hashkey,field)

    def __contains__(self,key):
        hashkey,field = self._get_hashname(key)
        res = self.connection.hget(hashkey,field)
        if res:
            return True
        return False

    def add(self,key):
        hashkey,field = self._get_hashname(key)
        self.connection.hset(hashkey,field,field)
        return

    def delete(self,key):
        hashkey,field = self._get_hashname(key)
        self.connection.hset(hashkey,field,field)
        self.connection.hdel(hashkey,field)
        return

    def clear(self):
        pipeline = self.connection.pipeline()
        keys = self.connection.keys(self.name+"*")
        for k in keys:
            pipeline.delete(k)

        pipeline.execute()
        return
