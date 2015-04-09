Transactions Example
====================

NOTE - this example requires apache cassandra version > 2.0 and the cassandra-driver-core version > 2.0.0

## Scenario

We want to add a number of products each with a quantity in stock. Orders will come in for random products and random buyers. 
This should allow for some of the products to sell out before others. When a order fails, an OUT OF STOCK message will be shown.

Also if a transaction fails due to the quantity changing in between the read of quantity and the order submission, then the following error will be shown

	update failed as capacity left is 871 not 872 for product P61


## Schema Setup
Note : This will drop the keyspace "datastax_referencedata_demo" and create a new one. All existing data will be lost. 

To specify contact points use the contactPoints command line parameter e.g. '-DcontactPoints=192.168.25.100,192.168.25.101'
The contact points can take mulitple points in the IP,IP,IP (no spaces).

To create the a single node cluster with replication factor of 1 for standard localhost setup, run the following

    mvn clean compile exec:java -Dexec.mainClass="com.datastax.demo.SchemaSetup"

To start the order processor, run

    mvn clean compile exec:java -Dexec.mainClass="com.datastax.transactions.Main" -Dload=true
    
The default is to use 3 threads but this can be changed by using the noOfThreads property. 

An example of running this with 10 threads and some custom contact points would be 

	mvn clean compile exec:java -Dexec.mainClass="com.datastax.transactions.Main" -Dload=true -DcontactPoints=cassandra1 -DnoOfThreads=10
	
Other properties that can be added are noOfProducts, inStock, noOfOrders

Another example we may want to have 100 products each with 1000 in stock. We can then simulate 100,000 orders over 10 apps using threads.
Some of these should fail as the product will be out of stock. 

	mvn clean compile exec:java -Dexec.mainClass="com.datastax.transactions.Main" -Dload=true -DnoOfThreads=10 -DnoOfProducts=100 -DinStock=1000 -DnoOfOrders=100000
	
To remove the tables and the schema, run the following.

    mvn clean compile exec:java -Dexec.mainClass="com.datastax.demo.SchemaTeardown"
    
## Testing Multiple clients from different JVMS

A good test of the transaction demo is to create different clients. This allows you to test the behaviour as if the clients were different app/web tiers instead of just mutiple threads from one JVM.

To do this, follow these steps.

1. Set up the schema

	mvn clean compile exec:java -Dexec.mainClass="com.datastax.demo.SchemaSetup"
    
2. Load the products first 
 
	mvn clean compile exec:java -Dexec.mainClass="com.datastax.transactions.Main" -DnoOfProducts=1 -DinStock=1000 -DloadOnly 
	
3. Create multiple terminal windows for as many clients as you wish to test and run (in each window)

	mvn exec:java -Dexec.mainClass="com.datastax.transactions.Main" -DnoOfThreads=1 -DnoOfProducts=1 -DinStock=1000 -DnoOfOrders=1000
	
These will need to be run at the same time to ensure there is contention and each client gets some products. 

In each of the processes, the total amount of successfully orders will be shown. They will add in the total no of in stock that was specified in the loading command (-DinStock).

	
