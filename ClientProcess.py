import socket, datetime, time, threading, sys
from multiprocessing import Process

KEEPALIVE_TIME_GAP = 2; #seconds
SHOST = '127.0.0.1'   # Symbolic name meaning all available interfaces
SPORT = 8888 # Arbitrary non-privileged port
CHOST = '127.0.0.1'   # Symbolic name meaning all available interfaces
CPORT = 9990 # Arbitrary non-privileged port
now=datetime.datetime.now()


def getExecuteTime():
    now=datetime.datetime.now()
    return time.mktime(now.timetuple()) 
              

class keepAliveThread (threading.Thread):
    def __init__(self,keepAliveTime,nextkeepAliveTime,doneFlag):
        self.process = None
        threading.Thread.__init__(self)
        self.keepAliveTime = keepAliveTime
        self.nextkeepAliveTime = nextkeepAliveTime
        self.doneFlag = doneFlag
    def run(self):
        # Connect to the server:
       
        client = socket.socket ( socket.AF_INET, socket.SOCK_STREAM )
        client.connect ( ( SHOST, SPORT ) )
        #infinite loop so that function do not terminate and thread do not end.
        while True:
            self.keepAliveTime =  getExecuteTime()
            if self.keepAliveTime >= self.nextkeepAliveTime:
                # Receiving from client
                # Listening for Keep Alive Status
                try:
                    self.nextkeepAliveTime = self.keepAliveTime+KEEPALIVE_TIME_GAP
                    if(self.doneFlag == False):
                        print "keep-alive:indexing"
                        client.send ('HeartBeat')
                    else:
                        print "keep-alive:indexing-done"
                        client.send ('Done')    
                   
                except socket.error:
                    #came out of loop
                    print "Master is down!!!"
                    client.close()
                    break
    def setDoneFlag(self,doneFlag):
        self.doneFlag = doneFlag
class DoIndexing (threading.Thread):
    def __init__(self,conn,doneFlag):
        threading.Thread.__init__(self)
        self.conn = conn
        self.doneFlag = doneFlag
    def run(self):
        # Connect to the server:
        # data = self.conn.recv(1024)
        # self.conn.close()
        # if data = "indexing"
            # start Thread keepAliveThread(keep-alive:indexing)
            # update StateDB every 5 seconds of its state and last indexed record
            # call indexingMethod to do indexing 
        # else if data = "writing"
            # start Thread keepAliveThread(keep-alive:writing)
            # update StateDB every 5 seconds of its state and last written record
            # call writingMethod to do writing 
        # HeartBeatThread.setDoneFlag(True)
        
        self.conn.close()
        keepAliveTime = getExecuteTime()
        nextkeepAliveTime = keepAliveTime+KEEPALIVE_TIME_GAP
        HeartBeatThread = keepAliveThread(keepAliveTime,nextkeepAliveTime,self.doneFlag)
        HeartBeatThread.start()
        while keepAliveTime <= nextkeepAliveTime+4:
            keepAliveTime = getExecuteTime()
            #print "test"
        HeartBeatThread.setDoneFlag(True)     
                   

if __name__ == '__main__':
    # Listen for master
    doneFlag = False
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    print 'Socket created'
     
    #Bind socket to local host and port
    try:
        s.bind((CHOST, CPORT))
    except socket.error as msg:
        print 'Bind failed. Error Code : ' + str(msg[0]) + ' Message ' + msg[1]
        sys.exit()
         
    print 'Socket bind complete'
     
    #Start listening on socket
    s.listen(5)
    print 'Socket now listening'
    while 1:
        #wait to accept a connection - blocking call
        conn, addr = s.accept()
        print 'Connected with ' + addr[0] + ':' + str(addr[1])
        

        DoIndexingT = DoIndexing(conn,doneFlag)
        # Start new Threads
        DoIndexingT.start()
        #DoIndexingT.join()
       
    s.close()
   
