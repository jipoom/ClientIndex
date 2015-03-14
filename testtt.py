import socket, time, threading, gzip,sys,re,os,datetime,errno
from pymongo import MongoClient
from xml.dom.minidom import DocumentType
from subprocess import check_output
def checkDBPerformance(host,port):
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

print checkDBPerformance("192.168.1.42",27017)