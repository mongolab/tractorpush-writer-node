//
// Tractor Push: Node.js, socket.io, Ruby, MongoDB tailed cursor demo
// app.js - writer that writes continuously to the collection.    Randomly
// chooses from three types to demonstrate schema flexibility of
// MongoDB as a queue.
//
// See also: https://github.com/mongolab/tractorpush-server
//

//
// ObjectLabs is the maker of MongoLab.com a MongoDB-as-a-Service
// provider.
//

//
// Copyright 2013 ObjectLabs Corp.  
//

// MIT License

// Permission is hereby granted, free of charge, to any person
// obtaining a copy of this software and associated documentation files
// (the "Software"), to deal in the Software without restriction,
// including without limitation the rights to use, copy, modify, merge,
// publish, distribute, sublicense, and/or sell copies of the Software,
// and to permit persons to whom the Software is furnished to do so,
// subject to the following conditions:  

// The above copyright notice and this permission notice shall be
// included in all copies or substantial portions of the Software. 

// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
// EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
// MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
// NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS
// BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN
// ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
// CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE. 

//

//
// NB: I don't provide a durable connection to mongodb that retries on
// failures. Instead I'm relying on a PaaS's durable restarts.
//

var fs = require('fs'), 
url = require('url'),
emitter = require('events').EventEmitter,
assert = require('assert'),

mongo = require('mongodb'),
QueryCommand = mongo.QueryCommand,
Cursor = mongo.Cursor,
Collection = mongo.Collection;

// Heroku-style environment variables
var uristring = process.env.MONGOLAB_URI || "mongodb://localhost/testdatabase" 
var mongoUrl = url.parse (uristring);

function includes(arr, obj) {
  for(var i=0; i<arr.length; i++) {
    if (arr[i] == obj) return true;
  }
  return false;
}

//
// Open mongo database connection
// A capped collection is needed to use tailable cursors
//
mongo.Db.connect (uristring, function (err, db) { 
  console.log ("Attempting connection to " + mongoUrl.protocol + "//" + mongoUrl.hostname + " (complete URL supressed).");
  db.collectionNames(function (err, colls) {
    if (includes(colls, 'messages')) {
      db.dropCollection("messages", function (err) {
	if (err) {
	  console.log ("Error dropping collection: ", err);
	  process.exit(2);
	} else {
	  createAndPopulate(db);
	}
      });
    } else {
      createAndPopulate(db);
    }
  });
});

function createAndPopulate(db) {
  db.createCollection ("messages", {capped: true, size: 8000000}, 
		       function (err, collection) {
			 if (err) {
			   console.log ("Error creating collection: ", err);
			   process.exit(3);
			 }

			 console.log ("Success connecting to " + mongoUrl.protocol + "//" + mongoUrl.hostname + ".");
			 
			 insertDocs (collection);
		       });
}

insert = 1;

function insertDocs (collection) {
  n = Math.floor(Math.random() * 3);
  doc = docs[n] 
  console.log ('n: ' + n);
  if (doc["_id"]) delete doc["_id"];
  time = new Date()
  doc.time = time.getTime()
  doc.ordinal = insert;

  if (insert == 2^30 -1)
    insert = 1;
  else
    insert = i + 1;
  
  collection.insert(doc);
  
  setTimeout(insertDocs, 1000, collection)
}

docs = [{'messagetype' : 'simple', 'ordinal' : 0, 'somename' : 'somevalue'}, 
        {'messagetype' : 'array', 'ordinal' : 0, 'somearray' : ['a', 'b', 'c', 'd']}, 
        {'messagetype' : 'complex', 'ordinal' :0, 'subdocument' : {'fname' : 'George', 'lname' : 'Washington', 'subproperty' : 'US-president'}}];



