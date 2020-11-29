using System;
using System.Collections.Concurrent;
using System.Linq;

namespace Client
{

    public class CacheEntry
    {
        public ObjectInfo Value { get; }

        // if cache is full, we remove the least recently accessed values
        public long Timestamp { get; set; }

        public CacheEntry(ObjectInfo value)
        {
            Value = value;
            Timestamp = GetTimestamp();
        }

        public void access()
        {
            Timestamp = GetTimestamp();
        }

        private long GetTimestamp()
        {
            return ((DateTimeOffset)DateTime.UtcNow).ToUnixTimeSeconds();
        }
    }

    public class Cache
    {
        private ConcurrentDictionary<ObjectId, CacheEntry> ObjectCache;

        private readonly int MAX_SIZE = 1024;

        // will be global lock since it is only used by potential concurrent threads
        // since the commands are sequential, there won't be much concurrency
        private readonly object CacheLock = new object();

        public Cache()
        {
            ObjectCache = new ConcurrentDictionary<ObjectId, CacheEntry>();
        }

        // stores object in the cache. 
        // returns true if got newer version of the object
        public bool RegisterObject(ObjectInfo newObj)
        {
            // object already exists => check if newer and update
            if (ObjectCache.ContainsKey(newObj.Key))
            {
                bool result = false;
                lock (CacheLock) {
                    ObjectInfo oldObj = (ObjectCache[newObj.Key]).Value;
                    if (CompareObjectVersion(oldObj.Version, newObj.Version) <= 0)
                    { // newObj is not outdated
                        ObjectCache[newObj.Key] = new CacheEntry(newObj);
                        result = true;
                    }
                }
                return result;
            }
            else // new object
            {
                if(ObjectCache.Count == MAX_SIZE)
                {
                    CleanCache();
                }
                ObjectCache[newObj.Key] = new CacheEntry(newObj);
            }
            return true;
        }

        // remove least recently used entries to clean up space
        private void CleanCache()
        {
            int numToRemove = MAX_SIZE / 10;
            lock (CacheLock)
            {
                foreach (var key in ObjectCache.Keys.ToList().OrderBy(key => ObjectCache[key].Timestamp).Take(numToRemove))
                {
                    ObjectCache.TryRemove(key, out var _);
                }
            }
        }

        // return -1, 0 or 1 if a is older, equal or newer than b, respectively (a-b)
        private int CompareObjectVersion(ObjectVersion a, ObjectVersion b)
        {
            if(a.Counter < b.Counter || (a.Counter == b.Counter && a.ClientId < b.ClientId))
            {
                return -1;
            }
            else if (a.Counter > b.Counter || (a.Counter == b.Counter && a.ClientId > b.ClientId))
            {
                return 1;
            }
            else
            {
                return 0;
            }
        }
    }
}