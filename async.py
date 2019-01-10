import asyncio
import uuid
import string
import random
import redis
import sys
from time import sleep

def random_msg(size=6, chars=string.ascii_uppercase + string.digits):
    return ''.join(random.choice(chars) for _ in range(size))

async def work(r, process_id):
    mode = 1 # 0 - reader, 1 - writer
    current_generator = r.get('generator')
    if current_generator and current_generator != process_id:
        mode = 0
    else:
        r.set('generator', process_id, 10)
        mode = 1

    while True:
        if mode == 1:
            r.set('generator', process_id, 10)
            msg = random_msg()
            r.rpush('messages', msg)
            print('Send: '+ msg)
        else:
            msg = r.rpop('messages')
            if msg is not None:
                print('Receive: ' + str(msg))
                random_int = random.randint(1, 20)
                if random_int == 1:
                    r.rpush('errors', msg)
            else:
                if r.get('generator') is None:
                    r.set('generator', process_id, 10)
                    mode = 1
        await asyncio.sleep(1)


process_id = str(uuid.uuid1())
r = redis.StrictRedis(host='localhost', port=6379, db=2)

if len(sys.argv)>1 and str(sys.argv[1]) == 'getErrors':
    print(r.lrange('errors', 0, -1))
    r.delete('errors')
    sys.exit()

loop = asyncio.get_event_loop()
try:
    asyncio.ensure_future(work(r, process_id))
    loop.run_forever()
except KeyboardInterrupt:
    pass
finally:
    print("Closing Loop")
    loop.close()