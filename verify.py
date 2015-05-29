#!/usr/bin/env python
import urllib2, json
from Queue import Queue
from threading import Thread
import hashlib
import sys


from tabulate import tabulate

source_db_url = "https://skimdb.npmjs.com/registry/"
dest_db_url = "http://path_to_second_repo/"

'''response = urllib2.urlopen(source_db_url + '_all_docs')
html = response.read()

packages = json.loads(html)
for line in packages['rows']:
    response = urllib2.urlopen(source_db_url + line['id'])
    package_meta = response.read()
    package_meta_json = json.loads(package_meta)
    if '_attachments' in package_meta_json:
        print line['id']
        for name, contents in package_meta_json['_attachments'].iteritems():
           #print type(attachement)
           print name, contents['digest'] 
           #print '    ' + str(package_meta_json['_attachments'])
        print "-"*20
    else:
        print line['id']
        print "-"*20
'''

def calculateMD5(string):
    m = hashlib.md5()
    m.update(string)
    return m.hexdigest()


def fetch_packeges(db_url, db_id):
    response = urllib2.urlopen(db_url + '_all_docs')
    html = response.read()
    packages = json.loads(html)
    #for line in packages['rows']:
    print packages['rows'][:1000]
    for line in packages['rows'][:1000]:
        response = urllib2.urlopen(db_url + line['id'])
        package_meta = response.read()
        package_meta_json = json.loads(package_meta)
        pkg_name = package_meta_json['_id']
        if '_attachments' in package_meta_json:
            for name, contents in package_meta_json['_attachments'].iteritems():
                q.put((db_id, pkg_name, name, contents['digest']))
        else:
            q.put((db_id, pkg_name, 'Null', 'Null'))
    q.put("done")

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
        if pkgNameAndAttachemen[1] == u'accountant':
            print pkgNameAndAttachemen
        if pkgNameAndAttachemen != "done":
            dbHash = pkgNameAndAttachemen[0]
            pkgName = pkgNameAndAttachemen[1]
            attName = pkgNameAndAttachemen[2]
            attHash = pkgNameAndAttachemen[3]
            element = (pkgName, attName, attHash)
            oppositeBaseHash = getOppositeBase(dbHash, dbContentPair)
            oppositeBase = dbContentPair[oppositeBaseHash]
            if oppositeBase:
                if element in oppositeBase:
                    existingPkgIndex = oppositeBase.index(element)
                    if pkgNameAndAttachemen[1] == u'accountant':
                        print "deleting!"
                        print "element = " + str(element)
                        print "dbHash = " + dbHash
                        print "oppositeBaseHash = " + oppositeBaseHash
                        print "existingPkgIndex = " + str(existingPkgIndex)
                    #print "dbContentPair[dbHash] = " + str(dbContentPair[dbHash])
                    #print "oppositeBase = " + str(oppositeBase)
                    #print dbContentPair[dbHash][existingPkgIndex]
                    del dbContentPair[oppositeBaseHash][existingPkgIndex]
                    #del oppositeBase[existingPkgIndex]
                else:
                    if pkgNameAndAttachemen[1] == u'accountant':
                        print "in firs else"
                    dbContentPair[dbHash].append(element)
            else:
                if pkgNameAndAttachemen[1] == u'accountant':
                    print "in second else"
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
                if record[1] == u"accountant":
                    print "record is: " + str(record)
                indexInFirst = FirstDatabase.index(record)
                indexInSecond = secondDatabase.index(record)
                print "Deleting indexes: first - " + str(indexInFirst) + "second: " + str(indexInSecond)
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
