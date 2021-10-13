import time


def timeit(func, log_name=''):
    """
    timer decorator
    can log results to log_file if string not empty (appends)
    """
    def timed(*args, **kw):
        ts = time.time()
        result = func(*args, **kw)
        te = time.time()
        hours, remainder = divmod(te - ts, 3600)
        minutes, seconds = divmod(remainder, 60)
        if log_name:
            with open(log_name, mode='a') as f:
                f.write(f'{func.__name__} took: {int(hours)}:{int(minutes)}:{seconds:2.6f}')
                f.close()
        else:
            print (f'{func.__name__} took:')
            print (f'{int(hours)} hours')
            print (f'{int(minutes)} minutes')
            print (f'{seconds:2.6f} seconds')
        return result
    return timed