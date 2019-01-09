import asyncio
import uuid
import string
import random
import redis
from time import sleep

def random_msg(size=6, chars=string.ascii_uppercase + string.digits):
    return ''.join(random.choice(chars) for _ in range(size))

async def work():
    process_id = str(uuid.uuid1())
    r = redis.StrictRedis(host='localhost', port=6379, db=2)

    mode = 1 # 0 - reader, 1 - writer
    current_generator = r.get('generator')
    if current_generator and current_generator != process_id:
        mode = 0
    else:
        r.set('generator', process_id, 10)
        mode = 1

    while True:
        if mode is 1:
            r.set('generator', process_id, 10)
            msg = random_msg()
            r.rpush('messages', msg)
            print('Send: '+ msg)
        else:
            msg = r.rpop('messages')
            if msg is not None:
                print('Receive: ' + str(msg))
            else:
                if r.get('generator') is None:
                    r.set('generator', process_id, 10)
                    mode = 1
        await asyncio.sleep(1)

loop = asyncio.get_event_loop()
try:
    asyncio.ensure_future(work())
    loop.run_forever()
except KeyboardInterrupt:
    pass
finally:
    print("Closing Loop")
    loop.close()