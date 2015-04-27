package com.datastax.titan;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.demo.utils.PropertyHelper;
import com.datastax.demo.utils.Timer;
import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.schema.TitanManagement;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.gremlin.java.GremlinPipeline;


public class RunQueries{
	private static Logger logger = LoggerFactory.getLogger(RunQueries.class);
	private TitanGraph graph;
	private Set<Vertex> recommendations = new HashSet<Vertex>();

	public RunQueries() {
		graph = TitanFactory
				.open("./src/main/resources/titan-cassandra.properties");
		
		Timer queryTimer = new Timer();
		Vertex user = runQueries();
		queryTimer.end();
		logger.info("Queries took " + queryTimer.getTimeTakenMillis() + "ms.");
		
		logger.info(recommendations.size() + " recommendations for user " + user.getProperty("name") +  " - " + recommendations.toString());
		graph.shutdown();
		logger.info("Finished.");
	}

	private Vertex runQueries() {
		Iterable<Vertex> vertices = graph.getVertices();
		
		Vertex userV = vertices.iterator().next();
		
		//Get the products that the user bought
		GremlinPipeline pipe = new GremlinPipeline();
		pipe.start(userV).out("bought");
		
		Set productsBought = new HashSet(pipe.toList());				
		getProducts(userV, productsBought);

		//For each product, find the users that bought it
		Iterator iterator = productsBought.iterator();
			
		while(iterator.hasNext()){
			Vertex product = graph.getVertex(iterator.next());

			//Only select users who have bought the product and are within 5 years of our selected user.
			GremlinPipeline similarUsers = new GremlinPipeline();			
			similarUsers.start(product).in("bought").interval("age", (Integer)userV.getProperty("age") - 5, 
					(Integer)userV.getProperty("age") + 5);
			
			Set similarUsersSet = new HashSet(similarUsers.toList());
			
			//For each similar user - find products 
			for (Object similarUserId : similarUsersSet) {
				
				Vertex similarUser = graph.getVertex(similarUserId);								
				if(similarUser.getId().equals(userV.getId())) continue;
				
				logger.info("User " + similarUser.getProperty("name") + " bought the same product as " + userV.getProperty("name"));
			
				GremlinPipeline productPipe = new GremlinPipeline();
				productPipe.start(similarUser).out("bought");
			
				//Add all the similar user products to the target users recommendations.
				recommendations.addAll(this.getProducts(similarUser, new HashSet(productPipe.toList())));
			}
		}	
		return userV;
	}	

	private Collection<Vertex> getProducts(Vertex user, Set products) {
		
		logger.info("User " + user.getProperty("name") + " bought : " + products.size() + " products.");
		
		Collection<Vertex> temp = new HashSet<Vertex>();
		
		StringBuffer buffer = new StringBuffer();
		for (Object product : products){
 			buffer.append(" -"+ ((Vertex)product).getProperty("name"));
 			
 			temp.add((Vertex)product);
		}
		
		logger.info("User " + user.getProperty("name") + "(" + user.getProperty("age") + ") bought product " + buffer.toString());
		return temp;
	}

	public static void main(String[] args) {
		new RunQueries();
	}
}
