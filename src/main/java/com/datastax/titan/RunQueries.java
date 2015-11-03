package com.datastax.titan;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.demo.utils.Timer;
import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;


public class RunQueries{
	private static Logger logger = LoggerFactory.getLogger(RunQueries.class);
	private TitanGraph graph;
	private Set<Vertex> recommendations = new HashSet<Vertex>();

	public RunQueries() {
		graph = TitanFactory
				.open("./src/main/resources/titan-cassandra.properties");
		
		Timer queryTimer = new Timer();
		
		GraphTraversal<Vertex, Map<String, Object>> valueMap = graph.traversal().V().valueMap();
		logger.info(valueMap.toString());

		Vertex user = runQueries();
		queryTimer.end();
		logger.info("Queries took " + queryTimer.getTimeTakenMillis() + "ms.");
		
		logger.info(recommendations.size() + " recommendations for user " + user.property("name") +  " - " + recommendations.toString());
		graph.close();
		logger.info("Finished.");
	}

	private Vertex runQueries() {
		Vertex userV = graph.traversal().V().has("name", "U2").next();
		
		GraphTraversalSource g = graph.traversal();

		//Get the products that the user bought, then get all users within 10 years and get their products as recommendations
		//g.V(user).out("bought").in().has("age", between(32,42)).out("bought").values("name");
		
		int min = (Integer.parseInt(userV.property("age").value().toString()) - 5);
		int max = (Integer.parseInt(userV.property("age").value().toString()) + 5);
		
		GraphTraversal<Vertex, Vertex> out = g.V(userV).out("bought").in().has("age", P.between(min,max)).out("bought");
		
		Set<Vertex> set = out.toSet();
		for (Vertex v : set){
			recommendations.add(v);
		}
		return userV;
	}	

	public static void main(String[] args) {
		new RunQueries();
	}
}
