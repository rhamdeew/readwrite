import uuid
import string
import random
import redis
from time import sleep

def id_generator(size=6, chars=string.ascii_uppercase + string.digits):
    return ''.join(random.choice(chars) for _ in range(size))

def main():
    process_id = uuid.uuid1()
    r = redis.StrictRedis(host='localhost', port=6379, db=2)

    mode = 1 # 0 - reader, 1 - writer
    if r.get('generator'):
        mode = 0
    else:
        r.set('generator', str(process_id))
        r.expire('generator', 10)
        mode = 1

    while True:
        if mode is 1:
            gen = id_generator()
            r.rpush('messages', gen)
            print('Send: '+ gen)
        else:
            msg = r.rpop('messages')
            if msg is not None:
                print('Receive: ' + str(msg))
            else:
                if r.get('generator') is None:
                    r.set('generator', str(process_id))
                    r.expire('generator', 10)
                    mode = 1
        sleep(1)

if __name__ == "__main__":
    main()