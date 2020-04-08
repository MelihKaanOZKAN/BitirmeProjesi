from utils.SMManager import SMManager
from utils.InstructManager import InstructManager
import subprocess, signal
import time,os, shlex
def spark():
    pc = "python /Users/melihozkan/Desktop/Projects/BitirmeProjesi/sparkManager.py --host 192.168.1.62 --port 1998 --sentimentId 1453 --master spark://192.168.1.33:7077 --method naiveBayes"
    FNULL = open(os.devnull, 'w')
    DETACHED_PROCESS = 0x00000008
    sub = subprocess.Popen(shlex.split(pc),  shell=False, stdin=None, stdout=None, stderr=None, close_fds=True).pid
    smman = SMManager()
    smman.add("1453", sub)
    sub = smman.get("1453")
    time.sleep(15)
    os.kill(sub, signal.SIGINT)
    print("Stop signal")
def write():
    a = InstructManager()
    a.writeInstructions("122","filter", ["news","breaking","trump"])

def test():
    import findspark
    print(findspark.find())

spark()