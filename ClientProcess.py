import socket, datetime, time, threading, sys
#import lib from indexScript
import glob,gzip,sys,timeit,re,os,datetime,urllib2,pymongo,errno

from multiprocessing import Process

KEEPALIVE_TIME_GAP = 2; #seconds
SHOST = '127.0.0.1'   # Symbolic name meaning all available interfaces
SPORT = 8888 # Arbitrary non-privileged port
CHOST = '127.0.0.1'   # Symbolic name meaning all available interfaces
CPORT = 9990 # Arbitrary non-privileged port
now=datetime.datetime.now()

#--------- From indexScript.py
def mkdir_p(path):
    try:
        os.makedirs(path)
    except OSError as exc: # Python >2.5
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else: raise

def openLogFile():
    now = datetime.datetime.now()
    year = now.strftime('%Y')
    month = now.strftime('%m')
    day = now.strftime('%d')
    filePath = '/home/logsearch/harbinger/log/'
    filePath += year + month +'/'
    fileName = filePath + year + month + day + '.log'
    try:
            logFile = open(fileName, 'a')
            return logFile
    except IOError:
            mkdir_p(filePath)
            raise
#--------- End of indexScript.py

#--------- Indexing method
def indexing(command):
    from pymongo import MongoClient
    client = MongoClient("mongodb://192.168.0.213:27017")
    db = client.logsearch
    #======== index mode ============
    print "Start Indexing"
    job_id = command[1]
    service = command[2]
    system = command[3]
    node = command[4]
    process = command[5]
    logPath = command[6]
    logType = command[7]
    logStartTag = command[8]
    logEndTag = command[9]
    msisdnRegex = command[10]
    dateHolder = command[11]
    dateRegex = command[12]
    dateFormat = command[13]
    timeRegex = command[14]
    timeFormat = command[15]
    mmin = command[16]
    interval = command[17]
    LastDoneRecord = command[18]
        # generate find command
    find_cmd = 'find ' + logPath + ' -type f'
    if mmin != "":
        find_cmd += ' -mmin -' + mmin
    if interval != "":
        find_cmd += ' -mmin +' + interval
    indexLogFile = openLogFile()
 ###################################################################
    
    dateTimeFormat = dateFormat + ' ' + timeFormat
    
    print "Find file with '"+ find_cmd +"'"
    # find file
    f = os.popen(find_cmd)
    files = f.readlines()
    #####################################################
    
    for file in files:
        try:
            today = datetime.datetime.now().strftime('%Y/%m/%d %H:%M:%S')
            #########################
            ## read file from path ##
            ###############################################
            file_path = file.rstrip('\n')
#            if mode == 'test':
#                print "PATH: " + file_path + "\n"
#                print '{0:6}  {1:11}  {2:19}  {3:8}  {4:6}'.format('index', 'msisdn', 'datetime', 'startTag', 'endTag')
#            else:
                # check file already indexed?
            collection = db.log_file
            cursor = collection.find_one({"service":service, "system":system, "node":node, "process":process, "path":file_path})
            if cursor: # already indexed, skip
                print file_path + ", This file is already indexed."
                indexLogFile.write( today + " Skip " + file_path + " , This file is already indexed\n")
                continue
            else: # not indexed add path and date to database
                print file_path + ", This file not already indexed."
                indexLogFile.write( today + " Index " + file_path + " , This file is not already indexed\n")
                collection.insert({"service":service, "system":system, "node":node, "process":process, "path":file_path, "datetime":today})
                
            
            collection = db.log_index
            if '.gz' in file_path:
                fileContent = gzip.open(file_path,'r')
            else:
                fileContent = open(file_path,'r')
            ###############################################
    
            ##########################
            ## define some variable ##
            ###############################################
            lineNumber = 0
            msisdn = ''
            date = ''
            time = ''
            index = 0
            startTag = 0
            endTag = 0
            showRecord = 0
            ###############################################
    
            #########################################
            ## if date in path, get date from path ##
            #################################################################
            if dateHolder == 'outside' and dateRegex.search(file_path) != None:
                date = dateRegex.search(file_path).group(1)
            #################################################################
    
            for line in fileContent:
                lineNumber += 1
                if showRecord == 110: # in test mode exit when already show 110 indexs
                    sys.exit(1)
                #############################
                ## Find msisdn, date, time ##
                #############################################################
                if msisdn == '' and msisdnRegex.search(line) != None:
                    msisdn = msisdnRegex.search(line).group(1)
                    index = lineNumber
                if dateHolder == 'inside' and date == '' and dateRegex.search(line) != None:
                    date = dateRegex.search(line).group(1)
                if time == '' and timeRegex.search(line) != None:
                    time = timeRegex.search(line).group(1)
                #############################################################
    
                ######################
                ## if multiline log ##
                #############################################################
                if logType == 'multiLine':
                    # when find start tag
                    if logStartTag.search(line) != None:
                        startTag = lineNumber
                    # when find end tag
                    if logEndTag.search(line) != None:
                        endTag = lineNumber
                        # if get all variable that require will print or insert in database
                        if msisdn != '' and date != '' and time != '':
                            # combine date time and change format
                            fullDateTime = date + ' ' + time
                            fullDateTime = datetime.datetime.strptime(fullDateTime, dateTimeFormat)
                            fullDateTime = fullDateTime.strftime('%Y/%m/%d %H:%M:%S')
#                            if mode == 'test':
#                                print '{0:6d}  {1:11}  {2:19}  {3:8d}  {4:6d}'.format(index, msisdn, fullDateTime, startTag, endTag)
#                                showRecord += 1
#                            else:
                            collection.insert({ "service": service,
                                                  "system": system,
                                                   "node": node,
                                                "process": process,
                                                   "path": file_path,
                                                   "msisdn": msisdn,
                                                   "index": index,
                                                   "datetime": fullDateTime,
                                                   "startTag": startTag,
                                                   "endTag": endTag })
                        # clear variable when found end tag
                        msisdn = ''
                        time = ''
                        startTag = 0
                        endTag = 0
                        if dateHolder == 'inside':
                            date = ''    # if date in log
                #############################################################
                #######################
                ## if singleline log ##
                #############################################################
                elif logType == 'singleLine':
                    # if get all variable that require will print or insert in database
                    if msisdn != '' and date != '' and time != '':
                        # combine date time and change format
                        fullDateTime = date + ' ' + time
                        fullDateTime = datetime.datetime.strptime(fullDateTime, dateTimeFormat)
                        fullDateTime = fullDateTime.strftime('%Y/%m/%d %H:%M:%S')
#                        if mode == 'test':
#                            print '{0:6d}  {1:11}  {2:19}  {3:8d}  {4:6d}'.format(index, msisdn, fullDateTime, index, index)
#                            showRecord += 1
#                        else:
                        collection.insert({ "service": service,
                                              "system": system,
                                               "node": node,
                                            "process": process,
                                               "path": file_path,
                                               "msisdn": msisdn,
                                               "index": index,
                                               "datetime": fullDateTime,
                                               "startTag": index,
                                               "endTag": index })
                    #clear variable every line
                    msisdn = ''
                    time = ''
                    if dateHolder == 'inside':
                        date = ''    #if date in log
                #############################################################
                
    
            fileContent.close()
            # for index test, index a file then exit
#            if mode == 'test':
#                break
        except IOError:
            print "I/O error"
    
#    if mode != 'test':
        indexLogFile.close()
    #stop = timeit.default_timer()
    #print stop-start
        
    
#--------- End of Indexing method

def getExecuteTime():
    now=datetime.datetime.now()
    return time.mktime(now.timetuple()) 
              

class keepAliveThread (threading.Thread):
    def __init__(self,keepAliveTime,nextkeepAliveTime,doneFlag,op):
        self.process = None
        threading.Thread.__init__(self)
        self.keepAliveTime = keepAliveTime
        self.nextkeepAliveTime = nextkeepAliveTime
        self.doneFlag = doneFlag
        self.op = op
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
                        if(self.op == "indexing"):
                            print "keep-alive:indexing"
                            client.send ('keep-alive:indexing')
                        else:
                            print "keep-alive:writing"
                            client.send ('keep-alive:writing')
                    else:
                        if(self.op == "indexing"):
                            print "keep-alive:indexing-done"
                            client.send ('keep-alive:indexing-done')  
                        else:  
                            print "keep-alive:writing-done"
                            client.send ('keep-alive:writing-done')                   
                except socket.error:
                    #came out of loop
                    print "Master is down!!!"
                    client.close()
                    break
    def setDoneFlag(self,doneFlag):
        self.doneFlag = doneFlag
class HandleMsg (threading.Thread):
    def __init__(self,doneFlag):
        threading.Thread.__init__(self)
       # self.conn = conn
        self.doneFlag = doneFlag
    def run(self):
        # Connect to the server:
        #data = self.conn.recv(1024)
        #self.conn.close()
        #Test command from the master
        data = "indexing##<job_id>##<service>##<system>##<node>##<process>\
        ##<path>##<log_type>##<logStartTag>##<logEndTag>##<msisdnRegex>\
        ##<dateHolder>##<dateRegex>##<dateFormat>##<timeRegex>\
        ##<timeFormat>##<mmin>##<interval>##LastDoneRecord=Line_num"
        # Split command
        cmd = data.split("##")
        
        # extract data to see 
        if cmd[0] == "indexing":
            # start Thread keepAliveThread(keep-alive:indexing)
            keepAliveTime = getExecuteTime()
            nextkeepAliveTime = keepAliveTime+KEEPALIVE_TIME_GAP
            op="indexing"
            HeartBeatThread = keepAliveThread(keepAliveTime,nextkeepAliveTime,self.doneFlag,op)
            HeartBeatThread.start()
            while keepAliveTime <= nextkeepAliveTime+4:
                keepAliveTime = getExecuteTime()
            #print "test"
            HeartBeatThread.setDoneFlag(True)  
            # update StateDB every 5 seconds of its state and last indexed record
            
            # call indexingMethod to do indexing 
            indexing(cmd)
        elif cmd[0] == "writing":
            print "writing"
            # start Thread keepAliveThread(keep-alive:writing)
            keepAliveTime = getExecuteTime()
            nextkeepAliveTime = keepAliveTime+KEEPALIVE_TIME_GAP
            op="writing"
            HeartBeatThread = keepAliveThread(keepAliveTime,nextkeepAliveTime,self.doneFlag,op)
            HeartBeatThread.start()
            while keepAliveTime <= nextkeepAliveTime+4:
                keepAliveTime = getExecuteTime()
            #print "test"
            HeartBeatThread.setDoneFlag(True)
            # update StateDB every 5 seconds of its state and last written record
            # call writingMethod to do writing 
            #writing(cmd)
        # HeartBeatThread.setDoneFlag(True)
        
#        self.conn.close()
   
                   

if __name__ == '__main__':

#    # Listen for master
#    doneFlag = False
#    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
#    print 'Socket created'
     
    #Bind socket to local host and port
#    try:
#        s.bind((CHOST, CPORT))
#    except socket.error as msg:
#        print 'Bind failed. Error Code : ' + str(msg[0]) + ' Message ' + msg[1]
#        sys.exit()
         
#    print 'Socket bind complete'
     
    #Start listening on socket
#   s.listen(5)
#   print 'Socket now listening'
                
#    while 1:
        #wait to accept a connection - blocking call
#        conn, addr = s.accept()
#        print 'Connected with ' + addr[0] + ':' + str(addr[1])
        

        #HandleMsgThread = HandleMsg(conn,doneFlag)
        doneFlag = False
        HandleMsgThread = HandleMsg(doneFlag)
        # Start new Threads
        HandleMsgThread.start()
        #DoIndexingT.join()
       
#    s.close()
   
