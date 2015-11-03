package com.datastax.titan;

import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.demo.utils.PropertyHelper;
import com.datastax.demo.utils.Timer;
import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.TitanTransaction;
import com.thinkaurelius.titan.core.TitanVertex;
import com.thinkaurelius.titan.core.schema.TitanManagement;


public class Main{
	private static Logger logger = LoggerFactory.getLogger(Main.class);
	private TitanGraph graph;

	public Main() {
		graph = TitanFactory
				.open("./src/main/resources/titan-cassandra.properties");
		
		String noOfUsersStr = PropertyHelper.getProperty("noOfUsers", "100");
		String noOfProductsStr = PropertyHelper.getProperty("noOfProducts", "100");

		int noOfUsers = Integer.parseInt(noOfUsersStr);
		int noOfProducts = Integer.parseInt(noOfProductsStr);

		List<Object> users = new ArrayList<Object>(noOfUsers);
		List<Object> products = new ArrayList<Object>(noOfProducts);
		
		Timer overallTimer = new Timer();		
		createUsers(noOfUsers, users);		
		createProducts(noOfProducts, products);
		
		int totalEdges = noOfProducts > noOfUsers ? noOfProducts : noOfUsers;		
		createEdges(totalEdges *4, users, products);
		
		overallTimer.end();
		logger.info("Overall took " + overallTimer.getTimeTakenSeconds() + "secs to complete.");
		
		Iterator<Vertex> vertices = graph.vertices();
				
		graph.close();
		logger.info("Finished.");
	}
	
	private void createEdges(int totalEdges, List<Object> users, List<Object> products) {
		logger.info("Adding " + totalEdges + " edges");
		
		TitanManagement managementSystem = graph.openManagement();
		managementSystem.makeEdgeLabel("bought");
		managementSystem.commit();
		
		TitanTransaction tx = graph.newTransaction();		
		
		for (int i = 0; i < totalEdges; i++) {			
			
			Iterator<Vertex> vertices = graph.vertices(users.get(new Double(Math.random() * users.size()).intValue()));
			Vertex userV = vertices.next();
			vertices = graph.vertices(products.get(new Double(Math.random() * products.size()).intValue()));
			Vertex productV = vertices.next();
			
			//if (userV.property("name").value().toString().equalsIgnoreCase("U2")){
				logger.info("Yes - Creating edge for " + userV.property("name").value().toString() + " and " + productV.property("name"));
		
			userV.addEdge("bought", productV, "weight", Math.random(), "time", new Date());
		}
		
		tx.commit();
	}

	private void createProducts(int noOfProducts, List<Object> products) {
		
		TitanManagement managementSystem = graph.openManagement();
		logger.info("Adding " + noOfProducts + " Products");
		for (int i = 0; i < noOfProducts; i++) {
			
			TitanVertex v = graph.addVertex();

			String product= "P" + i;

			v.property("name", product);
			v.property("price", (Math.random() * 99) + 1);			
			
			products.add(v.id());
			graph.tx().commit();
			
			if (i+1 % 1000 == 0){			
				logger.info("Total Products : " + i);
				managementSystem.commit();
				managementSystem = graph.openManagement();
			}
		}
		managementSystem.commit();
	}

	private void createUsers(int noOfUsers, List<Object> users) {
		TitanManagement managementSystem = graph.openManagement();
		
		logger.info("Adding " + noOfUsers + " Users");
		for (int i = 0; i < noOfUsers; i++) {
			
			graph.tx().open();
			TitanVertex v = graph.addVertex();

			String user = "U" + i;

			v.property("name", user);
			v.property("userid", i);		
			v.property("age", new Double(Math.random() * 60).intValue() + 18);
			
			graph.tx().commit();
			
			users.add(v.id());	
			
			if (i+1 % 1000 == 0){				
				logger.info("Total Users : " + i);
				managementSystem.commit();
				managementSystem = graph.openManagement();
			}
		}
		managementSystem.commit();
	}

	public static void main(String[] args) {
		new Main();
	}
}
