DataStax Titan Example
========================

The following example loads Users and Products into a Titan graph using Cassandra/DSE as the back end storage. 

First you will need to load a sample graph into Titan which will contain Users and Products. Each user will have bought random products. The sample query code is used to find recommendations for a random user using the following steps

1. find all products that the user bought
2. for each product, find all users that bought the same product and that are aged within 5 years of our sample user'age  
3. for all users that bought the same product and are within the age range, find all products they have bought 

This is a simply traversal with some filtering on age.

## Setup 
You will need to download [titan](https://github.com/thinkaurelius/titan/wiki/Downloads) and install to use this demo. You will also need to start elasticsearch (included with titan).

{TITAN_INTALL}/bin/elasticsearch

## Schema Setup
Note : This will use the keyspace "datastax_titan_demo". 

This uses the localhost for testing but you change the contact points for Cassandra/DSE in the titan-cassandra.properties file 
in the resources folder.

To load a sample graph

    mvn clean compile exec:java -Dexec.mainClass="com.datastax.titan.Main"
    
To run the queries to generate the recommendations run

	mvn clean compile exec:java -Dexec.mainClass="com.datastax.titan.RunQueries"
	
