from datetime import datetime

def print_log(method, str):
    print("[%s] %s: %s" % (method, \
                           datetime.now().strftime("%Y%m%d %H:%M:%S.%f"), \
                           str))