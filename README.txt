MPCS 53013
Author:Sirui Feng
Project: Chicago Crime Events

-Website:
http://104.197.248.161/siruif/crime_ward.html

-Goal:
This project intends to present the City of Chicago's crime facts 
from 2001 to present. Specifically, it allows users to lookup the 
average frequency of crime event by district number and month since 2001. 
The website updates the data every week.
The ultimate intention is to inform residents and city officers the most 
frequent crime event type in a particular area.

-Data:
The dataset is available at City of Chicago Data Portal: 
https://data.cityofchicago.org/Public-Safety/Crimes-2001-to-present/ijzp-q8t2
This dataset is 1.23G as of November 28, 11:01 PM.
It has the following columns:
'CASE#', 'DATE  OF OCCURRENCE', 'BLOCK', ' IUCR', ' PRIMARY DESCRIPTION', 
' SECONDARY DESCRIPTION', ' LOCATION DESCRIPTION', 'ARREST', 'DOMESTIC', 
'BEAT', 'WARD', 'FBI CD', 'X COORDINATE', 'Y COORDINATE', 'LATITUDE', 
'LONGITUDE', 'LOCATION'
I used remove_commas.py to remove commas in fields in the csv file.

-Streaming data:
The city also provides data in the most recent year:
https://data.cityofchicago.org/Public-Safety/Crimes-One-year-prior-to-present/x2n5-8w5q

It has the following columns:
CASE#,DATE  OF OCCURRENCE,BLOCK, IUCR, PRIMARY DESCRIPTION, 
SECONDARY DESCRIPTION, LOCATION DESCRIPTION,ARREST,DOMESTIC,
BEAT,WARD,FBI CD,X COORDINATE,Y COORDINATE,LATITUDE,LONGITUDE,LOCATION

-Architecture and Building Steps:
A. Batch Layer:
	0. Create a directory named as /siruif/final/crime/ on hdfs.
	1. Maven Install CrimeIngest.
	2. Java Run Configuration SerializeCrimeSummary.java with the path that contains the full 
	dataset as the argument to ingest the data into HDFS as thrift.
	3. Pig run readCrime.pig to process the data into Pig files.
	4. Create an empty HBase table called siruif_crime with a column family called crime by typing
	   command create 'siruif_crime', 'crime' in hbase shell.
	5. Pig run crimeCounts.pig to create the siruif_crime batch view in hadoop HBase

B. Serving Layer:
	0. Copy all of the .html, .jpg and .css files into /var/www/html/siruif in webserver.
	1. Copy all of the .pl files into /usr/lib/cgi/siruif in webserver.
	2. Make all of the .pl files executable by sudo chmod a+x /usr/lib/cgi/siruif/*.pl

C. Speed Layer:
	0. Create the Kafka topic siruif_crime by running the following command: 
	   kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic siruif-crime
	1. Build and run kafka-siruif-crime to ingest real-time flight data into Kafka.
	   https://data.cityofchicago.org/api/views/x2n5-8w5q/rows.csv?accessType=DOWNLOAD
	2. You can leave it running by: nohup java -cp uber-KafkaCrime-0.0.1-SNAPSHOT.jar edu.uchicago.mpcs.KafkaCrime.KafkaCrime &