#!/usr/bin/env python
# encoding: utf-8

import sys
import pymongo
from pymongo import Connection


def ConnectMongo():
  print 'Connecting to Mongo'
  try:
    connection = Connection() #connect default host and port
    #connection = Connection('localhost', 27017) #connect specific host and port
  except:
    print 'Connection failed!'


  return connection

def GetDatabase(connection, sDbName):
  #db = connection.test_database
  db = connection[sDbName]

  return db

def GetCollection(db, sCollectionName):
  #collection = db.test_collection
  collection = db[sCollectionName]

  return collection

def InsertDocument(db, sCollectionName):
  print '\nInserting document'
  db[sCollectionName].insert({'_id': 42, 'name': 'My Document', 'ids': [1,2,3], 'subdocument': {'a':2}})

  pass

def BatchInsert(db, sCollectionName):
  db[sCollectionName].insert([{'a':1}, {'a':2}])
  pass

def ListCollection(db, sCollectionName):
  print '\nListing - ' , sCollectionName, ' :'
  lColl = list(db[sCollectionName].find())

  for item in lColl:
    print item

  pass

def FindDocument(db, sCollectionName, sFind):
  print '\nFind ' , sFind, ' in ', sCollectionName

  #find returns a python iterator
  for str in db[sCollectionName].find({'name' : 'Aurora'}):
    print str

  pass

def RemoveData(db, sCollectionName):
  print '\nDeleting data'

  db[sCollectionName].remove({'a': {'$gt': 1}})

  pass

def UpdateData(db, sCollectionName):
  print '\nUpdating data'

  #it is also possible to reach a's sub attributes using 'a.b':1 etc
  db[sCollectionName].update({'a': 1}, {'$set': {'a': 7}})

  pass

def main():
  print 'it is started'

  #sDbName = 'tentips'
  sDbName = 'test'
  sCollectionName = 'unicorns'

  connection = ConnectMongo()
  db = GetDatabase(connection, sDbName)
  collection = GetCollection(db, sCollectionName)

  print '\nPrinting collection count:'
  print collection.count()
  print '\nPrinting first item in coll:'
  print collection.find_one()
  print '\nPrinting collection:'
  print collection.find()

  #InsertDocument(db, sCollectionName)

  #BatchInsert(db, sCollectionName)\

  ListCollection(db, sCollectionName)

  FindDocument(db, sCollectionName, 'hakan')

  #RemoveData(db, sCollectionName)

  ListCollection(db, sCollectionName)

  UpdateData(db, sCollectionName)

  ListCollection(db, sCollectionName)

  sys.exit()

if __name__ == '__main__':
    main()

