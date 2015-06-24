import traceback

def log_format_exception(ex=None):
    if ex and isinstance(ex, list):
        re = ''.join(ex)
        #re = ex[-1] + ex[0] + ''.join(list(ex[1:-1]))
    else:
        re = traceback.format_exc()
    return '\n> ' + re.replace('\n', '\n> ')