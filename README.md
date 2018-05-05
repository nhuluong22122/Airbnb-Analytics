# Airbnb Analytics

## Introduction

Using MongoDB and Java, this application can display user's requested information by querying to database of 3 nodes.

## Installation

Set up 3 nodes application: refer to take-home midterm

Maven Project: https://www.jetbrains.com/help/idea/maven-support.html

Install Mongo-Java-Driver: http://mongodb.github.io/mongo-java-driver/2.13/getting-started/installation-guide/

Sample Code to run: http://mongodb.github.io/mongo-java-driver/2.13/getting-started/quick-tour/#getting-started-with-java-driver

## Set Up For Localhost

We need 3 terminal tabs

First tab to run mongod
```
sudo mongod --port 27022
```
Second tab to import data
```
mongoimport --port 27022 --collection ratings --db airbnb --file airbnb-ratings-2017.json --jsonArray;
```
Third tab to run mongo and delete 2 rows (datasetid and recordsetid)
```
mongo --port 27022
show dbs
use airbnb
db.ratings.update({}, {$unset: {datasetid: 1, recordid:1}}, {multi:true});
```
