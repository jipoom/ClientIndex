import socket, datetime, time, threading, sys
from multiprocessing import Process

KEEPALIVE_TIME_GAP = 2; #seconds
HOST = '127.0.0.1'   # Symbolic name meaning all available interfaces
PORT = 8888 # Arbitrary non-privileged port
now=datetime.datetime.now()


def getExecuteTime():
    now=datetime.datetime.now()
    return time.mktime(now.timetuple()) 
              

class keepAliveThread (threading.Thread):
    def __init__(self,keepAliveTime,nextkeepAliveTime):
        self.process = None
        threading.Thread.__init__(self)
        self.keepAliveTime = keepAliveTime
        self.nextkeepAliveTime = nextkeepAliveTime
    def run(self):
        # Connect to the server:
       
        client = socket.socket ( socket.AF_INET, socket.SOCK_STREAM )
        client.connect ( ( HOST, PORT ) )
        #infinite loop so that function do not terminate and thread do not end.
        while True:
            self.keepAliveTime =  getExecuteTime()
            if self.keepAliveTime >= self.nextkeepAliveTime:
                # Receiving from client
                # Listening for Keep Alive Status
                try:
                    self.nextkeepAliveTime = self.keepAliveTime+KEEPALIVE_TIME_GAP
                    print "SendHeartBeat"
                    client.send ('HeartBeat')
                   
                except socket.error:
                    #came out of loop
                    print "Master is down!!!"
                    client.close()
                    break
            
def doIndexing():
    keepAliveTime = getExecuteTime()
    nextkeepAliveTime = keepAliveTime+KEEPALIVE_TIME_GAP
    HeartBeatThread = keepAliveThread(keepAliveTime,nextkeepAliveTime)
    HeartBeatThread.start()
    while True:
        keepAliveTime =  getExecuteTime()
        if keepAliveTime >= nextkeepAliveTime:
            nextkeepAliveTime = keepAliveTime+KEEPALIVE_TIME_GAP
            print "test"
            sys.stdout.flush()
        


if __name__ == '__main__':
    # Listen for master
    p = Process(target=doIndexing, args=())
    p.start()
    p.join()
