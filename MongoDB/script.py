import json
from pymongo import MongoClient

client = MongoClient()
db = client.sample
from datetime import datetime
print "Doing Read/Delete operations on JSON loaded in MongoDB"
print "Trying to find key:value pair in the file loaded in mongoDB"
#loadtime = datetime.now()
#load = db.samplecollect.find()
#for dump in load:
#    print(dump)
#print "time taken to load entire json file"
#print datetime.now() - loadtime

find_time = datetime.now()
mytest = db.samplecollect.find({"conformsTo": "https://project-open-data.cio.gov/v1.1/schema"})
exec1 = datetime.now() - find_time
print "Time taken to find the key:value pair is"
print(exec1)
mytests = db.samplecollect.find({"conformsTo": "https://project-open-data.cio.gov/v1.1/schema"}).count()
print "Number of occurences found are"
print(mytests)
print "Trying to find key:value pair which is not present in DB"
find_time2 = datetime.now()
another = db.samplecollect.find({"dataset.a": "public"})
exec2 = datetime.now() - find_time2
print "Not found: Execution time is"
print(exec2)
number = db.samplecollect.find({"dataset.a": "public"}).count()
print "Verifying count:"
print(number)



