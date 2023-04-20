from requests_futures.sessions import FuturesSession
from concurrent.futures import as_completed
import json
import time

key = 'V8lkz4WPVMZzldPaU0jbHHE3C2JX2JDI'
secret = '3SPHy0ZaaUYliDsc'

session = FuturesSession(max_workers=1)
for i in range(1914, 2020):
    url = f"https://api.nytimes.com/svc/archive/v1/{i}/1.json?api-key={key}"

    for f in as_completed([session.get(url)]):
        data = f.result().json()
        with open(f"./data/nyt/{i}.json", "w") as file:
            file.write(json.dumps(data))

    print(f"got {i}")
    time.sleep(12)