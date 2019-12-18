import os
import time
import string
import sys
import commands
from collections import namedtuple

_ntuple_diskusage = namedtuple('usage', 'total used free')

def disk_usage(path):
    """Return disk usage statistics about the given path.

    Returned valus is a named tuple with attributes 'total', 'used' and
    'free', which are the amount of total, used and free space, in gigabytes.
    """
    st = os.statvfs(path)
    free = st.f_bavail * st.f_frsize / 1e9
    total = st.f_blocks * st.f_frsize / 1e9
    used = (st.f_blocks - st.f_bfree) * st.f_frsize / 1e9
    return _ntuple_diskusage(total, used, free)

def get_cpumem(pid):
    d = [i for i in commands.getoutput("ps aux").split("\n")
        if i.split()[1] == str(pid)]
    return (float(d[0].split()[2]), float(d[0].split()[3])) if d else None

def get_db_connection_count():
   out = commands.getoutput("netstat -an | grep :9989 | wc -l")
   return out

if __name__ == '__main__':
    if not len(sys.argv) == 2 or not all(i in string.digits for i in sys.argv[1]):
        print("usage: %s PID" % sys.argv[0])
        exit(2)
    print("DATETIME,%CPU,%MEM,DISK(GB) for /data/tmp/tmp-docker,netstat -an | grep :9989 | wc -l")
    try:
        while True:
            x,y = get_cpumem(sys.argv[1])
	    z = time.strftime('%Y%m%d %H:%M:%S')
            disk_used = disk_usage('/data/tmp/tmp-docker')[1]
            db_connection_count = get_db_connection_count()

	    if not x:
                print("no such process")
                exit(1)
            print("%s,%.3f,%.3f,%.3f,%s" % (z,x,y,disk_used,db_connection_count))
	    sys.stdout.flush()
            time.sleep(5)
    except KeyboardInterrupt:
        # print
        exit(0)

