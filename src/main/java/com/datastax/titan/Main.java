package com.datastax.titan;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
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


public class Main{
	private static Logger logger = LoggerFactory.getLogger(Main.class);
	private TitanGraph graph;

	public Main() {
		graph = TitanFactory
				.open("./src/main/resources/titan-cassandra.properties");
		
		String noOfUsersStr = PropertyHelper.getProperty("noOfUsers", "10000");
		String noOfProductsStr = PropertyHelper.getProperty("noOfProducts", "1000");

		int noOfUsers = Integer.parseInt(noOfUsersStr);
		int noOfProducts = Integer.parseInt(noOfProductsStr);

		List<Object> users = new ArrayList<Object>(noOfUsers);
		List<Object> products = new ArrayList<Object>(noOfProducts);
		
		Timer overallTimer = new Timer();		
		createUsers(noOfUsers, users);		
		createProducts(noOfProducts, products);
		
		int totalEdges = noOfProducts > noOfUsers ? noOfProducts : noOfUsers;		
		createEdges(totalEdges *2, users, products);
		
		overallTimer.end();
		logger.info("Overall took " + overallTimer.getTimeTakenSeconds() + "secs to complete.");
		
		graph.shutdown();
		logger.info("Finished.");
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

	private void createEdges(int totalEdges, List<Object> users, List<Object> products) {
		logger.info("Adding " + totalEdges + " edges");
		
		TitanManagement managementSystem = graph.getManagementSystem();
		for (int i = 0; i < totalEdges; i++) {			
			
			Vertex userV = graph.getVertex(users.get(new Double(Math.random() * users.size()).intValue()));
			Vertex productV = graph.getVertex(products.get(new Double(Math.random() * products.size()).intValue()));
			
			Edge e = graph.addEdge(null, graph.getVertex(userV), graph.getVertex(productV), "bought");
			e.setProperty("weight", Math.random());
			e.setProperty("time", new Date());			
		}
		
		managementSystem.commit();
	}

	private void createProducts(int noOfProducts, List<Object> products) {
		
		TitanManagement managementSystem = graph.getManagementSystem();
		logger.info("Adding " + noOfProducts + " Products");
		for (int i = 0; i < noOfProducts; i++) {
			
			Vertex a = graph.addVertex(null);

			String product= "P" + i;

			a.setProperty("name", product);
			a.setProperty("price", (Math.random() * 99) + 1);			
			graph.commit();
			
			products.add(a.getId());	
			
			if (i+1 % 1000 == 0){			
				logger.info("Total Products : " + i);
				managementSystem.commit();
				managementSystem = graph.getManagementSystem();
			}
		}
		managementSystem.commit();
	}

	private void createUsers(int noOfUsers, List<Object> users) {
		TitanManagement managementSystem = graph.getManagementSystem();
		
		logger.info("Adding " + noOfUsers + " Users");
		for (int i = 0; i < noOfUsers; i++) {
			
			Vertex a = graph.addVertex(null);

			String user = "U" + i;

			a.setProperty("name", user);
			a.setProperty("userid", i);		
			a.setProperty("age", new Double(Math.random() * 60).intValue() + 18);
			graph.commit();
			
			users.add(a.getId());	
			
			if (i+1 % 1000 == 0){				
				logger.info("Total Users : " + i);
				managementSystem.commit();
				managementSystem = graph.getManagementSystem();
			}
		}
		managementSystem.commit();
	}

	public static void main(String[] args) {
		new Main();
	}
}
