DataStax Titan Example
========================

The following example loads Users and Products into a Titan graph using Cassandra/DSE as the back end storage. 

First you will need to load a sample graph into Titan. This will contain Users and Products. The sample query code is used to find 
recommendations for a random user using the following steps

1. the users products will be retrieved.
2. for each product, find all users that bought the product and that are aged within 5 years of our sample user'age  
3. for all desired users, find all products they have bought 

This is a simply traversal with some filtering on age.

## Schema Setup
Note : This will use the keyspace "datastax_titan_demo". 

This uses the localhost for testing but you change the contact points for Cassandra/DSE in the titan-cassandra.properties file 
in the resources folder.

To load a sample graph

    mvn clean compile exec:java -Dexec.mainClass="com.datastax.titan.Main"
    
To run the queries to generate the recommendations run

	mvn clean compile exec:java -Dexec.mainClass="com.datastax.titan.RunQueries"
	
