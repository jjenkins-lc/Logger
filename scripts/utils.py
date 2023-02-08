from functools import wraps
from time import time
import csv

def timeit(f):
    @wraps(f)
    def wrap(*args, **kwargs):
        time_start = time()
        result = f(*args, **kwargs)
        time_end = time()
        print("func: {} args:[{} ,{}] took {} seconds".format(f.__name__, args, kwargs, time_end-time_start))
        return result
    return wrap

def gen_test_file():
    with open('bigtest.csv', 'w') as f:
        fieldnames = ['id','test_text', 'time']
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        i=0
        while i < 10000000:
            writer.writerow({"id":"", "test_text": "sdfsdgdshgdskjg", "time" : str(datetime.now())})
            i+=1