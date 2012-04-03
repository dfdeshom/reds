import redis

class RedisLRUCache(object):
    """
    An LRU cache backed by redis zset and hash objects
    """
    def __init__(self,conn,cachename="lrucache",size=10):
        self.conn = conn
        self.lrulistname = cachename + "_list"
        self.countkey = cachename + "_count"
        self.cachename =  cachename
        self.size = size
                
    def trim(self):
        count = self.conn.zcard(self.lrulistname)
        excess = count - self.size
        if excess <= 0:
            return
        for i in range(excess):
            elements = self.conn.zrevrange(self.lrulistname,self.size-1,-1)
            #print 'removing ', elements
            #print 'current list', self.conn.zrange(self.lrulistname,0,-1,withscores=1)
            for el in elements:
                self.conn.hdel(self.cachename,el)
                self.conn.zrem(self.lrulistname,el)
        return
    
    def get(self,key):
        count = self.conn.incr(self.countkey)
        val = self.conn.hget(self.cachename,key)
        if val:
            self.conn.zadd(self.lrulistname,key,count)
            self.trim()
            return val
        return None

    def set(self,key,value):
        count = self.conn.incr(self.countkey)
        self.conn.hset(self.cachename,key,value)
        self.conn.zadd(self.lrulistname,key,count)
        self.trim()
        return

    def clear(self):
        self.conn.delete(self.cachename)
        self.conn.delete(self.lrulistname)
        self.conn.delete(self.countkey)
        
    def __contains__(self, key):
        res = self.get(key)
        if res:
            return True
        return False
    
    def __str__(self):
        elements = self.conn.hgetall(self.cachename)
        return str(elements)

    def debug_cache(self):
        """
        debug what's inside the cache
        """
        s = ''
        elements = self.conn.zrevrange(self.lrulistname,0,-1,withscores=1)
        s += "lru list: "+ str(elements)
        elements = self.conn.hgetall(self.cachename)
        s += "\nhash keys: " + str(elements)
        return s
