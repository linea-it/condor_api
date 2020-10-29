import json
import glob
import os
from pymemcache.client.base import Client
from pymemcache.client.base import PooledClient


class JsonSerde(object):
    def serialize(self, key, value):
        if isinstance(value, str):
            return value.encode('utf-8'), 1
        return json.dumps(value).encode('utf-8'), 2

    def deserialize(self, key, value, flags):
       if flags == 1:
           return value.decode('utf-8')
       if flags == 2:
           return json.loads(value.decode('utf-8'))
       raise Exception("Unknown serialization format")

db = PooledClient(('localhost', 11211), max_pool_size=5, serde=JsonSerde())


def clear_jsondb(jsondir):
    """ Clears json files in jsondb directory based on expired memcache keys """

    for item in glob.glob("{}/*.json".format(jsondir)):
        key = os.path.basename(item).replace(".json", "")
        if db.get(key) is None:
            print("The key {} not found".format(key))
            os.remove(item)
            lockitem = "{}.lock".format(item)
            if os.path.isfile(lockitem):
                os.remove(lockitem)
            print("{} removed".format(item))


if __name__ == '__main__':
    db.set('key', 'value:12345', expire=10)
    print(db.get('key'))
