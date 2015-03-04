import socket, time, threading, gzip,sys,re,os,datetime,errno
from pymongo import MongoClient
from xml.dom.minidom import DocumentType
# CONSTANT
KEEPALIVE_TIME_GAP = 2; #seconds
SHOST = '127.0.0.1'   # Symbolic name meaning all available interfaces
SPORT = 8888 # Arbitrary non-privileged port
CHOST = '127.0.0.1'   # Symbolic name meaning all available interfaces
CPORT = 9990 # Arbitrary non-privileged port
LOCAL_DB = '192.168.0.213'
LOCAL_PORT = 27017
ACTUAL_DB = '10.235.36.32'
ACTUAL_PORT = 2884
STATE_DB = "192.168.0.213"
STATE_DB_PORT = 27017
LOCAL_IP = ''

now=datetime.datetime.now()

def getLocalIP(s):
    try:
        client = s.getsockname()[0]
    except socket.error:
        client = "Unknown IP"
    return client

def getlogfileFromLocalDB():
    # Get log_file collection from Local DB
    mongoClient = MongoClient(LOCAL_DB, LOCAL_PORT)
    db = mongoClient.logsearch
    logfileCollection = db.log_file
    mongoClient.close()
    # return log_file collection
    return logfileCollection

def getlogindexFromLocalDB():
    # Get log_index collection from Local DB
    mongoClient = MongoClient(LOCAL_DB, LOCAL_PORT)
    db = mongoClient.logsearch
    logindexCollection = db.log_index
    mongoClient.close()
    # return log_index collection
    return logindexCollection

def getlogindexFromActualDB():
    # Get log_index collection from Actual DB
    mongoClient = MongoClient(ACTUAL_DB, ACTUAL_PORT)
    db = mongoClient.logsearch
    logindexCollection = db.log_index
    mongoClient.close()
    # return log_index collection
    return logindexCollection

def getlogindexFromOtherDB(IP,PORT):
    # Get log_index collection from other DB
    mongoClient = MongoClient(IP, PORT)
    db = mongoClient.logsearch
    logindexCollection = db.log_index
    mongoClient.close()
    # return log_index collection
    return logindexCollection

def getRecordFromStateDB(IP,PORT):
    # Get last record from state DB
    mongoClient = MongoClient(IP, STATE_DB_PORT)
    db = mongoClient.logsearch
    stateCollection = db.StateDB_state
    mongoClient.close()
    # return string containing jobID:state:last_record:node
    print "getRecordStateDB"
    return stateCollection

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
    """
    indexing##<job_id>## <state_db_ip:PORT> ##<service>##<system>##<node>##<process
    >##<path>##<log_type>##<logStartTag>##<logEndTag>##<msisdnRegex>##<dat
    eHolder>##<dateRegex>##<dateFormat>##<timeRegex>##<timeFormat>##<mmin
    >##<interval>## lastIndexedFile ##LastDoneRecord=Line_num
    """
    
    # start Thread keepAliveThread(keep-alive:indexing)
    keepAliveTime = getExecuteTime()
    nextkeepAliveTime = keepAliveTime+KEEPALIVE_TIME_GAP
    op="indexing"           
    HeartBeatThread = keepAliveThread(keepAliveTime,nextkeepAliveTime,False,False,op,command[1])
    HeartBeatThread.start()
    #HeartBeatThread.setDoneFlag(True)
    #HeartBeatThread.setStopFlag(True)
    
    #======== index mode ============
    print "Start Indexing"
    if(command[8] == "singleLine"):
        job_id = command[1]
        state_db_ip = (command[2].split(":"))[0]
        state_db_port = (command[2].split(":"))[1]
        service = command[3]
        system = command[4]
        node = command[5]
        process = command[6]
        logPath = command[7]
        logType = command[8]
        msisdnRegex = command[9]
        dateHolder = command[10]
        dateRegex = command[11]
        dateFormat = command[12]
        timeRegex = command[13]
        timeFormat = command[14]
        mmin = command[15]
        interval = command[16]
        lastIndexedFile = command[17]
        LastDoneRecord = command[18]
    elif(command[8] == "multiLine"):
        job_id = command[1]
        state_db_ip = (command[2].split(":"))[0]
        state_db_port = (command[2].split(":"))[1]
        service = command[3]
        system = command[4]
        node = command[5]
        process = command[6]
        logPath = command[7]
        logType = command[8]
        logStartTag = command[9]
        logEndTag = command[10]
        msisdnRegex = command[11]
        dateHolder = command[12]
        dateRegex = command[13]
        dateFormat = command[14]
        timeRegex = command[15]
        timeFormat = command[16]
        mmin = command[17]
        interval = command[18]
        lastIndexedFile = command[19]
        LastDoneRecord = command[20]
    # init state DB  
    state_collection = getRecordFromStateDB(state_db_ip,state_db_port)
    state_collection.insert({ "jobID": command[1],
                          "state": "indexing",
                           "lastFileName": "",
                           "lastDoneRecord": 0,
                           "db_ip": LOCAL_IP
                           })     
    
    
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
#          today = datetime.datetime.now().strftime('%Y/%m/%d %H:%M:%S')
            #########################
            ## read file from path ##
            ###############################################
            file_path = file.rstrip('\n')
#            if mode == 'test':
#                print "PATH: " + file_path + "\n"
#                print '{0:6}  {1:11}  {2:19}  {3:8}  {4:6}'.format('index', 'msisdn', 'datetime', 'startTag', 'endTag')
#            else:
                # check file already indexed?
#            collection = getlogfileFromLocalDB()
#            cursor = collection.find_one({"service":service, "system":system, "node":node, "process":process, "path":file_path})
#            if cursor: # already indexed, skip
#                print file_path + ", This file is already indexed."
#                indexLogFile.write( today + " Skip " + file_path + " , This file is already indexed\n")
#                continue
#            else: # not indexed add path and date to database
#                print file_path + ", This file not already indexed."
#                indexLogFile.write( today + " Index " + file_path + " , This file is not already indexed\n")
#                collection.insert({"service":service, "system":system, "node":node, "process":process, "path":file_path, "datetime":today})
                
            collection = getlogindexFromLocalDB()
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
                
                # To resume unfinished job
                # Check the file name and set line number
                if lastIndexedFile in file :
                    lineNumber = LastDoneRecord
                    
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
                                                   "endTag": endTag,
                                                   "job_id" : job_id })
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
                                               "endTag": index,
                                               "job_id" : job_id })
                    #clear variable every line
                    msisdn = ''
                    time = ''
                    if dateHolder == 'inside':
                        date = ''    #if date in log
                #############################################################
                
            if lineNumber%1000 ==0 :
                #state_collection = getRecordFromStateDB(state_db_ip,state_db_port)
                if state_collection.find({'jobID': job_id}).count > 0:
                    state_collection.update({'jobID': job_id}, {"$set": {'state': "indexing", 'lastFileName':file,
                                                                     'lastDoneRecord':lineNumber,'db_ip':LOCAL_IP}}) 
                else:
                    state_collection.insert({ "jobID": job_id,
                          "state": "indexing",
                           "lastFileName": file,
                           "lastDoneRecord": 0,
                           "db_ip": LOCAL_IP
                           }) 
                                          
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

#--------- Writing method
def writing(command):
    """writing##<job_id>##<state_db_ip:state_db_port>##<main_db_ip:main_db_port>##<db_ip:db_port>##lastDoneRecord"""
   
    # start Thread keepAliveThread(keep-alive:writing)
    keepAliveTime = getExecuteTime()
    nextkeepAliveTime = keepAliveTime+KEEPALIVE_TIME_GAP
    op="writing"
    HeartBeatThread = keepAliveThread(keepAliveTime,nextkeepAliveTime,False,False,op,command[1])
    HeartBeatThread.start()    
    #HeartBeatThread.setDoneFlag(True)
    #HeartBeatThread.setStopFlag(True)
    #cmd = command.split("##")
    job_id = command[1]
    state_db_ip = (command[2].split(":"))[0]
    state_db_port = int((command[2].split(":"))[1])
    main_db_ip = (command[3].split(":"))[0]
    main_db_port = int((command[3].split(":"))[1])
    db_ip = (command[4].split(":"))[0] 
    db_port = int((command[4].split(":"))[1])
    i = 0        
    
    # init state DB  
    
    state_collection = getRecordFromStateDB(state_db_ip,state_db_port)
    state_collection.insert({ "jobID": job_id,
                          "state": "writing",
                           "lastDonefile": "",
                           "lastDoneRecord": 0,
                           "db_ip": db_ip
                           }) 
        
    #Connect to Other database servers
    db_collection = getlogindexFromOtherDB(db_ip,db_port)
    cursor_ = db_collection.find()
    for cursor in cursor_:
        i = i+1
        service = cursor['service']
        system = cursor['system']
        node = cursor['node']
        process = cursor['process']
        file_path = cursor['path']
        msisdn = re.compile(cursor['msisdn'])
        index = re.compile(cursor['index'])
        fullDateTime = cursor['datetime']
        startTag = re.compile(cursor['startTag'])
        endTag = re.compile(cursor['endTag'])
        
        acutal_collection = getlogindexFromOtherDB(main_db_ip,main_db_port)
        acutal_collection.insert({ "service": service,
                          "system": system,
                           "node": node,
                        "process": process,
                           "path": file_path,
                           "msisdn": msisdn,
                           "index": index,
                           "datetime": fullDateTime,
                           "startTag": startTag,
                           "endTag": endTag,
                           "job_id" : job_id })
        
        #remove a record
        db_collection.remove({ "service": service,
                          "system": system,
                           "node": node,
                        "process": process,
                           "path": file_path,
                           "msisdn": msisdn,
                           "index": index,
                           "datetime": fullDateTime,
                           "startTag": startTag,
                           "endTag": endTag,
                           "job_id" : job_id })
        
        #state_collection = getRecordFromStateDB(state_db_ip,state_db_port)
        if state_collection.find({'jobID': job_id}).count > 0:
            state_collection.update({'jobID': job_id}, {"$set": {'state': "writing", 'lastDoneRecord':i}})
        else:
            state_collection.insert({ "jobID": job_id,
                          "state": "writing",
                           "lastDoneRecord": file,
                           "lastDoneRecord": i
                           }) 
           
#--------- End of Writing method

def getExecuteTime():
    now=datetime.datetime.now()
    return time.mktime(now.timetuple()) 
              

class keepAliveThread (threading.Thread):
    def __init__(self,keepAliveTime,nextkeepAliveTime,doneFlag,stopFlag,op,jobid):
        self.process = None
        threading.Thread.__init__(self)
        self.keepAliveTime = keepAliveTime
        self.nextkeepAliveTime = nextkeepAliveTime
        self.doneFlag = doneFlag
        self.stopFlag = stopFlag
        self.op = op
        self.jobid = jobid
    def run(self):
        # Connect to the server:
       
        client = socket.socket ( socket.AF_INET, socket.SOCK_STREAM )
        client.connect ( ( SHOST, SPORT ) )
        LOCAL_IP = getLocalIP(client)

        #infinite loop so that function do not terminate and thread do not end.
        while True:
            self.keepAliveTime =  getExecuteTime()
            if self.keepAliveTime >= self.nextkeepAliveTime:
                # Receiving from client
                # Listening for Keep Alive Status
                try:
                    if(self.stopFlag == True):
                        client.close()
                        break
                    self.nextkeepAliveTime = self.keepAliveTime+KEEPALIVE_TIME_GAP
                    if(self.doneFlag == False):
                        if(self.op == "indexing"):
                            print self.jobid+"##indexing"
                            client.send (self.jobid+'##indexing')
                        else:
                            print self.jobid+"##writing"
                            client.send (self.jobid+'##writing')
                    else:
                        if(self.op == "indexing"):
                            print self.jobid+"##indexing-done"
                            client.send (self.jobid+'##indexing-done')  
                        else:  
                            print self.jobid+"##writing-done"
                            client.send (self.jobid+'##writing-done')                   
                except socket.error:
                    #came out of loop
                    print "Master is down!!!"
                    client.close()
                    break
    def setDoneFlag(self,doneFlag):
        self.doneFlag = doneFlag
    def setStopFlag(self,stopFlag):
        self.stopFlag = stopFlag
        
class HandleMsg (threading.Thread):
    def __init__(self,conn,doneFlag):
        threading.Thread.__init__(self)
        self.doneFlag = doneFlag
        self.conn = conn
    def run(self):
        # Connect to the server:
        data =  self.conn.recv(1024)
        self.conn.close()
        #Test command from the master
        #data = "indexing##12345## <state_db_ip:123> ##<service>##<system>##<node>##<process\
        #        >##<path>##<log_type>##<logStartTag>##<logEndTag>##<msisdnRegex>##<dat\
        #        eHolder>##<dateRegex>##<dateFormat>##<timeRegex>##<timeFormat>##<mmin\
        #        >##<interval>## lastIndexedFile ##LastDoneRecord=Line_num"
        # Split command
        print data
        cmd = data.split("##")
        jobid = cmd[1]
        # extract data to see 
        if cmd[0] == "indexing":
            # while keepAliveTime <= nextkeepAliveTime+10:
            #    keepAliveTime = getExecuteTime()
            #print "test"
            
            # update StateDB every 5 seconds of its state and last indexed record
            
            # call indexingMethod to do indexing 
            indexing(cmd)
            #HeartBeatThread.setDoneFlag(True)  
        elif cmd[0] == "writing":
            print "writing"
           
            #while keepAliveTime <= nextkeepAliveTime+10:
            #    keepAliveTime = getExecuteTime()
            # print "test"
            # update StateDB every 5 seconds of its state and last written record
            # call writingMethod to do writing 
            writing(cmd)
            #HeartBeatThread.setDoneFlag(True)
        # HeartBeatThread.setDoneFlag(True)
        
#        self.conn.close()
   
                   

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
        
        #HandleMsgThread = HandleMsg(conn,doneFlag)
        HandleMsgThread = HandleMsg(conn, doneFlag)
        # Start new Threads
        HandleMsgThread.start()
        #DoIndexingT.join()
       
    s.close()
   
