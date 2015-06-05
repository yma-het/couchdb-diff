#!/usr/bin/env python
import urllib2, json
from Queue import Queue
from threading import Thread
import hashlib
import sys
from tabulate import tabulate

import tornado.ioloop
from tornado.httpclient import AsyncHTTPClient, HTTPRequest

from time import sleep

source_db_url = "https://skimdb.npmjs.com/registry/"
#dest_db_url = "http://path_to_second_repo/"



def calculateMD5(string):
    m = hashlib.md5()
    m.update(string)
    return m.hexdigest()

def getFileName(string):
    lastSlashPos = string.rfind("/")
    return string[lastSlashPos + 1:]


class RequestHandler(object):
    def __init__(self, fetcherInstance):
        self.fetcherInstance = fetcherInstance
        #print "Inited RequestHandler"
    def __call__(self, response):
        if response.error:
            print "Error:", response.error
        else:
            print "Got callback!!!"
            package_meta = response.body
            package_meta_json = json.loads(package_meta)
            pkg_name = package_meta_json['_id']
            db_id = self.fetcherInstance.db_id
            elementIsInQuee = False

            if 'versions' in package_meta_json:
                for version, contents in package_meta_json['versions'].iteritems():
                    if 'dist' in contents:
                        if ('tarball' in contents['dist']) and ('shasum' in contents['dist']):
                            filename = getFileName(contents['dist']['tarball'])
                            shasum = contents['dist']['shasum']
                            q.put((db_id, pkg_name, filename, shasum))
                            elementIsInQuee = True
                if not elementIsInQuee:
                    q.put((db_id, pkg_name, 'Null', 'Null'))

        self.fetcherInstance.pendingRequests -= 1
        self.fetcherInstance.getNextItem()
            
        #q.put


class AsyncPkgMetaFetcher(object):
    def __init__(self, packageList, db_url, db_id):
        #print "Init AsyncPkgMetaFetcher(object)"
        self.http_client = AsyncHTTPClient()
        #self.packageList = packageList[:1000]
        self.packageList = packageList
        self.db_url = db_url
        self.db_id = db_id
        self.numOfParallelConnections = 1000
        self.pendingRequests = 0

    def run(self):
       for i in range(self.numOfParallelConnections):
           #if len(self.packageList) > 0:
           #    pkgName = self.packageList[0]['id']
           #    self.packageList = self.packageList[1:]
           #    self.pendingRequests += 1
           #    print self.db_url + pkgName
           #    callbackObj = RequestHandler(self)
           #    request = HTTPRequest(self.db_url + pkgName, connect_timeout=300.0, request_timeout=300.0)
           #    self.http_client.fetch(request, callbackObj)
           # else:
           #     #if all two thread is done, stop event loop
           #     pass
           self.getNextItem()

    def getNextItem(self):
        if len(self.packageList) > 0:
            pkgName = self.packageList[0]['id']
            self.packageList = self.packageList[1:]
            self.pendingRequests += 1
            print self.db_url + pkgName
            callbackObj = RequestHandler(self)
            request = HTTPRequest(self.db_url + pkgName, connect_timeout=300.0, request_timeout=300.0)
            self.http_client.fetch(request, callbackObj)
        elif (self.pendingRequests == 0):
            #while not (self.pendingRequests == 0):
            #    print "Waiting for " + str(self.pendingRequests) + "remaining requests to be done..."
            #    sleep(10)
            #self.http_client.close()
            q.put("done")


eventLoopIsRunning = False

def fetch_packeges(db_url, db_id):
    global eventLoopIsRunning
    response = urllib2.urlopen(db_url + '_all_docs')
    html = response.read()
    packages = json.loads(html)
    #for line in packages['rows']:
    #print packages['rows'][:1000]
    #for line in packages['rows'][:10]:


    print "Creating instance of AsyncPkgMetaFetcher"
    fetcher = AsyncPkgMetaFetcher(packages['rows'], db_url, db_id)
    fetcher.run()
    if not eventLoopIsRunning:
        eventLoopIsRunning = True # no race condition 'cause Python is single-threaded
        tornado.ioloop.IOLoop.instance().start()

   #http_client = AsyncHTTPClient() # we initialize our http client instance
   #http_client.fetch("http://ip.jsontest.com", handle_request) # here we try
   #                 # to fetch an url and delegate its response to callback
   #tornado.ioloop.IOLoop.instance().start() # start the tornado ioloop to
                    # listen for events


    '''if db_id == "7b500ede70bef3f3b1ddc63ee1238d6e":
        listOfPackages = packages['rows'][:100]
    else:
        listOfPackages = packages['rows']

    for line in listOfPackages:
        try:
            response = urllib2.urlopen(db_url + line['id'], None, 5)
            package_meta = response.read()
            package_meta_json = json.loads(package_meta)
            pkg_name = package_meta_json['_id']
            elementIsInQuee = False

            if 'versions' in package_meta_json:
                for version, contents in package_meta_json['versions'].iteritems():
                    if 'dist' in contents:
                        if ('tarball' in contents['dist']) and ('shasum' in contents['dist']):
                            filename = getFileName(contents['dist']['tarball'])
                            shasum = contents['dist']['shasum']
                            q.put((db_id, pkg_name, filename, shasum))
                            elementIsInQuee = True
                if not elementIsInQuee:
                    q.put((db_id, pkg_name, 'Null', 'Null'))

            

            #if '_attachments' in package_meta_json:
            #    for name, contents in package_meta_json['_attachments'].iteritems():
            #        q.put((db_id, pkg_name, name, contents['digest']))
            #else:
            #    q.put((db_id, pkg_name, 'Null', 'Null'))

        except urllib2.HTTPError as e:
            print "urllib2.HTTPError has been raised!"
            print e.code
            print e.read() 
    q.put("done")'''

def getOppositeBase(currentDB, dbStruct):
    for db_hash in dbStruct:
        if db_hash != currentDB:
            #print "Current database is: " + currentDB
            #print "Returning: " + db_hash
            return db_hash
    else:
        return False


def processPackages():
    threadsDone = 0
    #while True:
    #    print "Doing!"
    while threadsDone < 2:
        #print "Processing quee!"
        pkgNameAndAttachemen = q.get()
        #if pkgNameAndAttachemen[1] == u'accountant':
        #    print pkgNameAndAttachemen
        if pkgNameAndAttachemen != "done":
            dbHash = pkgNameAndAttachemen[0]
            pkgName = pkgNameAndAttachemen[1]
            attName = pkgNameAndAttachemen[2]
            attHash = pkgNameAndAttachemen[3]
            element = (pkgName, attName, attHash)
            oppositeBaseHash = getOppositeBase(dbHash, dbContentPair)
            oppositeBase = dbContentPair[oppositeBaseHash]
            print "Processing " + dbHash + " => " + pkgName
            if oppositeBase:
                if element in oppositeBase:
                    existingPkgIndex = oppositeBase.index(element)
                    #if pkgNameAndAttachemen[1] == u'accountant':
                    #    print "deleting!"
                    #    print "element = " + str(element)
                    #    print "dbHash = " + dbHash
                    #    print "oppositeBaseHash = " + oppositeBaseHash
                    #    print "existingPkgIndex = " + str(existingPkgIndex)
                    #print "dbContentPair[dbHash] = " + str(dbContentPair[dbHash])
                    #print "oppositeBase = " + str(oppositeBase)
                    #print dbContentPair[dbHash][existingPkgIndex]
                    del dbContentPair[oppositeBaseHash][existingPkgIndex]
                    #del oppositeBase[existingPkgIndex]
                else:
                    #if pkgNameAndAttachemen[1] == u'accountant':
                    #    print "in firs else"
                    dbContentPair[dbHash].append(element)
            else:
                #if pkgNameAndAttachemen[1] == u'accountant':
                #    print "in second else"
                dbContentPair[dbHash].append(element)
            #print pkgNameAndAttachemen[0]
            #print pkgNameAndAttachemen[1] + " => " + pkgNameAndAttachemen[2]
        else:
            threadsDone += 1
        #sys.exit(1)
    else:
        # here we must handle all post-fetch operations
        first_hash = dbContentPair.keys()[0]
        firstDatabase = dbContentPair[first_hash]
        second_hash = dbContentPair.keys()[1]
        secondDatabase = dbContentPair[second_hash]
        for record in firstDatabase[:]:
            if record in secondDatabase:
                #if record[1] == u"accountant":
                #    print "record is: " + str(record)
                indexInFirst = FirstDatabase.index(record)
                indexInSecond = secondDatabase.index(record)
                #print "Deleting indexes: first - " + str(indexInFirst) + "second: " + str(indexInSecond)
                del firstDatabase[indexInFirst]
                del secondDatabase[indexInSecond]

        q.task_done()

               

q = Queue()


def startFetcherThread(db_url):
    databaseId = calculateMD5(db_url)
    dbContentPair[databaseId] = []
    fetcherThread = Thread(target=fetch_packeges, args=[db_url, databaseId])
    fetcherThread.setDaemon(True)
    fetcherThread.start()

# global
dbContentPair = {}
startFetcherThread(source_db_url)
startFetcherThread(dest_db_url)

#getterThread = Thread(target=processPackages)
#getterThread.setDaemon(True)
#getterThread.start()

#q.join()

processPackages()
#print len(dbContentPair[dbContentPair.keys()[0]])
#print len(dbContentPair[dbContentPair.keys()[1]])
print "Printing difference:"
#print "First"
print tabulate(dbContentPair, headers="keys")
