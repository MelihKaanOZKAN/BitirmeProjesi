from utils.SMManager import SMManager
from utils.InstructManager import InstructManager
import time
def spark():
    sm = SMManager()
    sm.startStream("7a1e693a-821b-4924-b341-52e2bcc8276e", "filter", ["trump"])
    time.sleep(30)
    sm.stopStream("1480")
    print("Stop Signal")
    while sm.isSparkContextRunning("1480"):
        print("running")
        time.sleep(10)
    print("Exited")

def write():
    a = InstructManager()
    a.writeInstructions("122","filter", ["news","breaking","trump"])

def test():
     from utils.hdfsClient import client
     a = client()
     print(a.read("/logs/sentimentlog_1480.log"))
spark()