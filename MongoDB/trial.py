import json
from pymongo import MongoClient

client = MongoClient()
db = client.test
from datetime import datetime
print "Inserting documents to MongoDB using PyMongo from the script"
start=datetime.now()
for i in range(0,10000):
	result = db.restaurants.insert_one(
    	{
        	"address": {
            	"street": " Avenue",
            	"zipcode": "10075",
            	"building": "1480",
            	"coord": [-73.9557413, 40.7720266]
        	},
		"id" : i,
        	"borough": "Manhattan",
        	"cuisine": "Italian",
	        "grades": [
	            {
        	        "date": datetime.strptime("2014-10-01", "%Y-%m-%d"),
        	        "grade": "A",
        	        "score": 11
        	    },
        	    {
        	        "date": datetime.strptime("2014-01-16", "%Y-%m-%d"),
        	        "grade": "B",
        	        "score": 17
        	    }
        	],
        	"name": "Vella",
        	"restaurant_id": "41704620"
    	}
	)
result.inserted_id
print "Time taken to write 10000 records to MongoDB is:"
print datetime.now() - start
start1 = datetime.now()
print "Doing a read operation"
cursor = db.restaurants.find({"id": 1})
total = db.restaurants.find({"id": 1}).count()
print(total)
print "Time taken to find record"
print datetime.now() - start1
start2 = datetime.now()
print "Delete a key:Value pair"
delete = db.restaurants.remove({"borough": "Manhattan"})
print(delete)
print "Trying to find the deleted record"
check=db.restaurants.find({"borough": "Manhattan"}).count()
print "Time taken to perform delete and verify is:"
print datetime.now() - start2
print "Number of occurences after deleting key:Value pair is"
print(check)


