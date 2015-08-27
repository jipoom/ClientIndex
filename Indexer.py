import socket, time, threading, gzip,sys,re,os,datetime,errno
from pymongo import MongoClient
from xml.dom.minidom import DocumentType
from subprocess import check_output
#netifaces
import netifaces as ni
from netifaces import AF_INET, AF_INET6, AF_LINK


# CONSTANT
KEEPALIVE_TIME_GAP = 2; #seconds
#SHOST = sys.argv[1]   # Symbolic name meaning all available interfaces
#SPORT = int(sys.argv[2]) # Arbitrary non-privileged port
CHOST = ''   # Symbolic name meaning all available interfaces
CPORT = 9990 # Arbitrary non-privileged port
LOCAL_DB = '127.0.0.1'
LOCAL_PORT = 27017
ACTUAL_DB = '192.168.1.129'
ACTUAL_PORT = 2884
STATE_DB = "192.168.1.42"
STATE_DB_PORT = 27017
ETHER_PORT = sys.argv[1]

now=datetime.datetime.now()

def sleeper(seconds):
    time.sleep(seconds)

def getLocalIP():
    return ni.ifaddresses(ETHER_PORT)[AF_INET][0]['addr']
    #ip = "192.168.1.43"
    #return ip

def getlogfileFromActualDB(ip,port):
    # Get log_file collection from Local DB
    mongoClient = MongoClient(ip, port)
    db = mongoClient.logsearch
    logfileCollection = db.log_file
    mongoClient.close()
    # return log_file collection
    return logfileCollection

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
    # print "getRecordStateDB"
    return stateCollection

def DBTransactions(host,port):
    # check DB workload
    #cmd = "mongostat -host "+host+" -port "+str(port)+" -n 1"
    #output = os.popen(cmd)
    output = check_output(["mongostat", "-host",host,"-port",str(port),"-n", "1"])
    rate = output.split('\n')
    # get first column of the result (insert rate)
    insertRate = rate[2][:6]
    queryRate = rate[2][6:13]
    updateRate = rate[2][13:20]
    deleteRate = rate[2][21:27]
    #print output
    #print insertRate
    #print queryRate
    #print updateRate
    #print deleteRate
    return (int)(insertRate.translate(None, ' *'))+(int)(queryRate.translate(None, ' *'))+(int)(updateRate.translate(None, ' *'))+(int)(deleteRate.translate(None, ' *'))

    
def setIndexerToDead(ip,port,jobID):
    mongoClient = MongoClient(ip, port)
    db = mongoClient.logsearch
    indexerStateCollection = db.indexer_state
    indexerStateCollection.update_many({'jobID': jobID}, {"$set": {'state': 'dead'}})  

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
    hour = now.strftime('%H')    
    filePath = '/home/logsearch/harbinger/indexer/log/'
    filePath += year + month +'/'
    fileName = filePath + year + month + day + hour+'00.log'
    try:
            logFile = open(fileName, 'a')
            return logFile
    except IOError:
            mkdir_p(filePath)
            logFile = open(fileName, 'a')
            #return logFile
            return logFile
#--------- End of indexScript.py

#--------- Indexing method
def indexing(command,data):
    """
    indexing##<job_id>## <state_db_ip:PORT> ##<service>##<system>##<node>##<process
    >##<path>##<log_type>##<logStartTag>##<logEndTag>##<msisdnRegex>##<dat
    eHolder>##<dateRegex>##<dateFormat>##<timeRegex>##<timeFormat>##<mmin
    >##<interval>## lastIndexedFile ##LastDoneRecord=Line_num
    """
    
    # start Thread keepAliveThread(keep-alive:indexing)
    #keepAliveTime = getExecuteTime()
    #nextkeepAliveTime = keepAliveTime+KEEPALIVE_TIME_GAP
    op="indexing"           
    indexLogFile = openLogFile()
    # Log cmd got from Master
    indexLogFile.write("["+datetime.datetime.fromtimestamp(int(getExecuteTime())).strftime('%Y-%m-%d %H:%M:%S')+"] Task: " + data+"\n")
    #======== index mode ============
    print "Start Indexing"
    if(command[8] == "singleLine"):
        job_id = command[1]
        state_db_ip = (command[2].split(":"))[0]
        state_db_port = int((command[2].split(":"))[1])
        service = command[3]
        system = command[4]
        node = command[5]
        process = command[6]
        logPath = command[7]
        logType = command[8]
        msisdnRegex = re.compile(command[9])
        dateHolder = command[10]
        dateRegex = re.compile(command[11])
        dateFormat = command[12]
        timeRegex = re.compile(command[13])
        timeFormat = command[14]
        mmin = command[15]
        interval = command[16]
        lastIndexedFile = command[17]
        LastDoneRecord = command[18]
        main_db_ip = (command[19].split(":"))[0]
        main_db_port = int((command[19].split(":"))[1])
        masterListIP = (command[20].split(":"))[0]
        masterListPort = int((command[20].split(":"))[1])
    elif(command[8] == "multiLine"):
        job_id = command[1]
        state_db_ip = (command[2].split(":"))[0]
        state_db_port = int((command[2].split(":"))[1])
        service = command[3]
        system = command[4]
        node = command[5]
        process = command[6]
        logPath = command[7]
        logType = command[8]
        logStartTag = re.compile(command[9])
        logEndTag = re.compile(command[10])
        msisdnRegex = re.compile(command[11])
        dateHolder = command[12]
        dateRegex = re.compile(command[13])
        dateFormat = command[14]
        timeRegex = re.compile(command[15])
        timeFormat = command[16]
        mmin = command[17]
        interval = command[18]
        lastIndexedFile = command[19]
        LastDoneRecord = command[20]
        main_db_ip = (command[21].split(":"))[0]
        main_db_port = int((command[21].split(":"))[1])
        masterListIP = (command[22].split(":"))[0]
        masterListPort = int((command[22].split(":"))[1])
    
    #start keep-alive with MasterListener
    HeartBeatThread = keepAliveThread(False,False,op,command[1],masterListIP,masterListPort)
    HeartBeatThread.start()
    try:    
        # generate find command
        find_cmd = 'find ' + logPath + ' -type f'
        if mmin != "":
            find_cmd += ' -mmin -' + mmin
        if interval != "":
            find_cmd += ' -mmin +' + interval
        
    ###################################################################
        
        dateTimeFormat = dateFormat + ' ' + timeFormat
        
        print "["+datetime.datetime.fromtimestamp(int(getExecuteTime())).strftime('%Y-%m-%d %H:%M:%S')+"] Find file with '"+ find_cmd +"'"
        indexLogFile.write("["+datetime.datetime.fromtimestamp(int(getExecuteTime())).strftime('%Y-%m-%d %H:%M:%S')+"] Find file with '"+ find_cmd +"'"+"\n")
     
        # find file
        f = os.popen(find_cmd)
        files = f.readlines()
        if len(files) == 0:
            #####################################################Sleeper()
            sleeper(3)
            e = sys.exc_info()[0]
            HeartBeatThread.setDoneFlag(True)
            print( "["+datetime.datetime.fromtimestamp(int(getExecuteTime())).strftime('%Y-%m-%d %H:%M:%S')+"] <p>Error: %s</p>" % e )
            indexLogFile.write(  "["+datetime.datetime.fromtimestamp(int(getExecuteTime())).strftime('%Y-%m-%d %H:%M:%S')+"] <p>Error: %s</p>" % e +"\n")
        else:             
            #startFile = False
            startLine = False
            indexedList = []
            logFileList = []
            lastLineList = []
            recordCount = -1
            #error = False
            # collection = getlogindexFromLocalDB()
            logFilecollection = getlogfileFromActualDB(main_db_ip,main_db_port)
            for file in files:
                if STOP_INDEXING:
                    break;
                #if lastIndexedFile == file:
                #    startFile = True
                #if lastIndexedFile == "" or startFile:
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
                    
                    cursor = logFilecollection.find_one({"service":service, "system":system, "node":node, "process":process, "path":file_path})
                    if cursor: # already indexed, skip
                        print "["+datetime.datetime.fromtimestamp(int(getExecuteTime())).strftime('%Y-%m-%d %H:%M:%S')+"] "+file_path + ", This file is already indexed."
                        indexLogFile.write("["+datetime.datetime.fromtimestamp(int(getExecuteTime())).strftime('%Y-%m-%d %H:%M:%S')+"] Job: "+job_id+" Skip: " + file_path+"\n")
                        continue
                    else: # not indexed add path and date to database
                        print "["+datetime.datetime.fromtimestamp(int(getExecuteTime())).strftime('%Y-%m-%d %H:%M:%S')+"] "+file_path + ", This file not already indexed."
                        # JobID
                        indexLogFile.write("["+datetime.datetime.fromtimestamp(int(getExecuteTime())).strftime('%Y-%m-%d %H:%M:%S')+"] Job: "+job_id+" Indexed: " + file_path+"\n")
                        logFileDict = {
                               'service': service,
                               'system':system,
                               'node':node,
                               'process':process,
                               'path': file_path,
                               'datetime': today
                               }
                        #logFileList.append(logFileDict)
                        
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
                    tm= ''
                    index = 0
                    startTag = 0
                    endTag = 0
                    showRecord = 0
                    isIndexed = False
                    ###############################################
            
                    #########################################
                    ## if date in path, get date from path ##
                    #################################################################
                    if dateHolder == 'outside' and dateRegex.search(file_path) != None:
                        date = dateRegex.search(file_path).group(1)
                    #################################################################
                    #print "***************************"
                    #print fileContent
                    #print lastIndexedFile.rstrip('\n') 
                    #print file_path
                    #print "***************************"
                    for line in fileContent:        
                        if STOP_INDEXING == True:            
                            break;
                        lineNumber = int(lineNumber) + 1
                        # check if the current line match last done record of the last indexed file
                        if int(LastDoneRecord)+1 == lineNumber and lastIndexedFile.rstrip('\n')  == file_path and lastIndexedFile != '' :
                            #print lineNumber
                            startLine = True
                        if LastDoneRecord == "-1" or startLine or lastIndexedFile.rstrip('\n')  != file_path or lastIndexedFile == '':
                            # To resume unfinished job
                            # Check the file name and set line number
                            #if lastIndexedFile in file and lastIndexedFile != '':
                            #    lineNumber = int(LastDoneRecord)
                                
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
                            if tm== '' and timeRegex.search(line) != None:
                                tm= timeRegex.search(line).group(1)
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
                                    if msisdn != '' and date != '' and tm!= '':
                                        # combine date time and change format
                                        fullDateTime = date + ' ' + tm
                                        fullDateTime = datetime.datetime.strptime(fullDateTime, dateTimeFormat)
                                        fullDateTime = fullDateTime.strftime('%Y/%m/%d %H:%M:%S')
            #                            if mode == 'test':
            #                                print '{0:6d}  {1:11}  {2:19}  {3:8d}  {4:6d}'.format(index, msisdn, fullDateTime, startTag, endTag)
            #                                showRecord += 1
            #                            else:
                                        indexedDict = {
                                                       "service": service,
                                                              "system": system,
                                                               "node": node,
                                                            "process": process,
                                                               "path": file_path,
                                                               "msisdn": msisdn,
                                                               "index": index,
                                                               "datetime": fullDateTime,
                                                               "startTag": startTag,
                                                               "endTag": endTag,
                                                               "job_id" : job_id,
                                                               "lastLine" : lineNumber
                                                       }
                                        indexedList.append(indexedDict)
                                        recordCount = recordCount+1  
                                        isIndexed = True
                                        #collection.insert({ "service": service,
                                        #                      "system": system,
                                        #                       "node": node,
                                        #                    "process": process,
                                        #                       "path": file_path,
                                        #                       "msisdn": msisdn,
                                        #                       "index": index,
                                        #                       "datetime": fullDateTime,
                                        #                       "startTag": startTag,
                                        #                       "endTag": endTag,
                                        #                       "job_id" : job_id })
                                    # clear variable when found end tag
                                    msisdn = ''
                                    tm= ''
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
                                if msisdn != '' and date != '' and tm!= '':
                                    # combine date time and change format
                                    fullDateTime = date + ' ' + tm
                                    fullDateTime = datetime.datetime.strptime(fullDateTime, dateTimeFormat)
                                    fullDateTime = fullDateTime.strftime('%Y/%m/%d %H:%M:%S')
            #                        if mode == 'test':
            #                            print '{0:6d}  {1:11}  {2:19}  {3:8d}  {4:6d}'.format(index, msisdn, fullDateTime, index, index)
            #                            showRecord += 1
            #                        else:
                                    indexedDict = {
                                                       "service": service,
                                                          "system": system,
                                                           "node": node,
                                                        "process": process,
                                                           "path": file_path,
                                                           "msisdn": msisdn,
                                                           "index": index,
                                                           "datetime": fullDateTime,
                                                           "startTag": index,
                                                           "endTag": index,
                                                           "job_id" : job_id,
                                                           "lastLine" : lineNumber
                                                       }
                                    indexedList.append(indexedDict)
                                    recordCount = recordCount+1 
                                    isIndexed = True 
                                    #collection.insert({ "service": service,
                                    #                      "system": system,
                                    #                       "node": node,
                                    #                    "process": process,
                                    #                       "path": file_path,
                                    #                       "msisdn": msisdn,
                                    #                       "index": index,
                                    #                       "datetime": fullDateTime,
                                    #                       "startTag": index,
                                    #                       "endTag": index,
                                    #                       "job_id" : job_id })
                                #clear variable every line
                                msisdn = ''
                                tm= ''
                                if dateHolder == 'inside':
                                    date = ''    #if date in log
                            #############################################################
                            # print lineNumber
                            #if lineNumber%1000 ==0 :
                            #    state_collection = getRecordFromStateDB(state_db_ip,state_db_port)
                            #    state_collection.update({'jobID': job_id}, {"$set": {'state': "indexing", 'lastFileName':file,
                            #                                                        'lastDoneRecord':lineNumber,'db_ip':LOCAL_IP}}) 
                                                      
                    fileContent.close()
                    lastLineList.append(recordCount)
                    # insert only specified type of log
                    if isIndexed:
                        logFileList.append(logFileDict)
                    # for index test, index a file then exit
        #            if mode == 'test':
        #                break
                except:
                    sleeper(3)
                    HeartBeatThread.setStopFlag(True)
                    raise
                    print "["+datetime.datetime.fromtimestamp(int(getExecuteTime())).strftime('%Y-%m-%d %H:%M:%S')+"] Error exists"              
            #collection = getlogindexFromLocalDB()
            
            #print lastLineList
            #if len(logFileList) > 0:
            #    print logFileList[0]
            # check if DB is too busy
            while 1:
                if STOP_INDEXING:
                    break;
                if(DBTransactions(main_db_ip, main_db_port) < 4500):
                    break
                else:
                    print "["+datetime.datetime.fromtimestamp(int(getExecuteTime())).strftime('%Y-%m-%d %H:%M:%S')+"] Indexed_DB : "+job_id+" : too many concurrent transactions" 
                    sleeper(3)
            acutal_collection = getlogindexFromOtherDB(main_db_ip,main_db_port)
            state_collection = getRecordFromStateDB(state_db_ip,state_db_port)
            i=0
            j=0
            count=0
            temp=[]
            isFirstTime = True;
            isDuplicate = True;
            filePath = "";
            while (i<len(indexedList)):
                if STOP_INDEXING:
                    break;    
                # Upsert to DB
                # This is to prevent inserting duplicate data
                if isFirstTime:
                    temp = acutal_collection.find({"$and": [{"system":indexedList[i]['system']},{"node":indexedList[i]['node']},{"process":indexedList[i]['process']},{"path": indexedList[i]['path']}]}).sort([['endTag',-1]])
                    count = temp.count()
                    isFirstTime=False
                if count > 0 and isDuplicate:
                    if indexedList[i]['endTag'] <= temp[0]['endTag']:
                        i=i+1;
                        continue;
                    else:
                        isDuplicate=False
                acutal_collection.insert({"service": indexedList[i]['service'],
                                                          "system": indexedList[i]['system'],
                                                           "node": indexedList[i]['node'],
                                                        "process": indexedList[i]['process'],
                                                           "path": indexedList[i]['path'],
                                                           "msisdn": indexedList[i]['msisdn'],
                                                           "index": indexedList[i]['index'],
                                                           "datetime": indexedList[i]['datetime'],
                                                           "startTag": indexedList[i]['startTag'],
                                                           "endTag": indexedList[i]['endTag'],
                                                           "job_id" : indexedList[i]['job_id'] })
                if i in lastLineList:
                    logFilecollection.insert({"service": logFileList[j]['service'],
                                                          "system": logFileList[j]['system'],
                                                           "node": logFileList[j]['node'],
                                                        "process": logFileList[j]['process'],
                                                           "path": logFileList[j]['path'],
                                                           "datetime": logFileList[j]['datetime'] }) 
                    print "["+datetime.datetime.fromtimestamp(int(getExecuteTime())).strftime('%Y-%m-%d %H:%M:%S')+"] Done writing: "+logFileList[j]['path']
                    j = j+1
                if filePath != indexedList[i]['path']:#i%500==0 :
                    filePath = indexedList[i]['path']
                    state_collection.update({'jobID': job_id}, {"$set": {'state': "indexing", 'lastFileName':indexedList[i]['path'],
                                                                                     'lastDoneRecord':indexedList[i]['lastLine'],'db_ip':LOCAL_IP, 'executionTime':int(time.time())}}) 
                if i%500==0:
                    print "["+datetime.datetime.fromtimestamp(int(getExecuteTime())).strftime('%Y-%m-%d %H:%M:%S')+"] Indexing line: " +str(indexedList[i]['lastLine'])+": "+logFileList[j]['path']
                #indexLogFile.write( today + " Job: "+job_id+" Done Writing: line " + str(indexedList[i]['lastLine'])+" of "+logFileList[j]['path'])                                                                   
                i = i+1
            if not STOP_INDEXING:
                indexLogFile.write( "["+datetime.datetime.fromtimestamp(int(getExecuteTime())).strftime('%Y-%m-%d %H:%M:%S')+"] Job: "+job_id+" Done"+"\n")
                indexLogFile.close()
            else:
                print "["+datetime.datetime.fromtimestamp(int(getExecuteTime())).strftime('%Y-%m-%d %H:%M:%S')+"] Master Listener is down!!!"
                #set StateDB to dead                 
                setIndexerToDead(main_db_ip, main_db_port,job_id)
            HeartBeatThread.setDoneFlag(True) 
              
            #    if mode != 'test':
                
    except:
    #     ###Sleeper(3)
        sleeper(3)
        e = sys.exc_info()[0]
        HeartBeatThread.setStopFlag(True)
        raise
        print( "<p>Error: %s</p>" % e )
    #stop = timeit.default_timer()
    #print stop-start
        
    
#--------- End of Indexing method


def getExecuteTime():
    now=datetime.datetime.now()
    return time.mktime(now.timetuple()) 
              

class keepAliveThread (threading.Thread):
    def __init__(self,doneFlag,stopFlag,op,jobid,masterListIP,masterListPort):
        self.process = None
        threading.Thread.__init__(self)
        self.doneFlag = doneFlag
        self.stopFlag = stopFlag
        self.op = op
        self.jobid = jobid
        self.masterListIP = masterListIP
        self.masterListPort = masterListPort
    def run(self):
        # Connect to the server:
        client = socket.socket ( socket.AF_INET, socket.SOCK_STREAM )
        try:
            client.connect ( ( self.masterListIP, self.masterListPort ) )
            LOCAL_IP = getLocalIP()
            global STOP_INDEXING
            STOP_INDEXING = False
            #infinite loop so that function do not terminate and thread do not end.
            while True:
            #    self.keepAliveTime =  getExecuteTime()
            #    if self.keepAliveTime >= self.nextkeepAliveTime:
                    # Receiving from client
                    # Listening for Keep Alive Status
                try:
                    if(self.stopFlag == True):
                        client.close()
                        break
                    #self.nextkeepAliveTime = self.keepAliveTime+KEEPALIVE_TIME_GAP
                    if(self.doneFlag == False):
                        if(self.op == "indexing"):
                            #print "["+datetime.datetime.fromtimestamp(int(getExecuteTime())).strftime('%Y-%m-%d %H:%M:%S')+"] self.jobid+"##indexing"
                            client.send (self.jobid+'##indexing')
                        else:
                            print self.jobid+"##writing"
                            client.send (self.jobid+'##writing')
                    else:
                        if(self.op == "indexing"):
                            print "["+datetime.datetime.fromtimestamp(int(getExecuteTime())).strftime('%Y-%m-%d %H:%M:%S')+"] "+self.jobid+"##indexing-done"
                            client.send (self.jobid+'##indexing-done')  
                            client.close()
                            break
                        else:  
                            print self.jobid+"##writing-done"
                            client.send (self.jobid+'##writing-done') 
                            client.close()                  
                except socket.error:
                    #came out of loop
                    print "["+datetime.datetime.fromtimestamp(int(getExecuteTime())).strftime('%Y-%m-%d %H:%M:%S')+"] Master Bye!!!"
                    client.close()
                    raise
                    STOP_INDEXING = True
                    break
                sleeper(KEEPALIVE_TIME_GAP)
        except socket.error:
            #stop
            STOP_INDEXING = True            
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
        print "["+datetime.datetime.fromtimestamp(int(getExecuteTime())).strftime('%Y-%m-%d %H:%M:%S')+"] Got Task from Master"
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
            indexing(cmd,data)
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
   
LOCAL_IP = getLocalIP()            
STOP_INDEXING = False
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
   
