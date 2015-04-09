package com.datastax.titan;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.demo.utils.PropertyHelper;
import com.datastax.demo.utils.Timer;
import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.schema.TitanManagement;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;

public class MainMemory {
	private static Logger logger = LoggerFactory.getLogger(MainMemory.class);

	public MainMemory() {
		Configuration conf = new BaseConfiguration();
		conf.setProperty("storage.directory", "/tmp/graph");
		conf.setProperty("storage.backend", "inmemory");
		TitanGraph graph = TitanFactory.open(conf);
		
		String noOfVertexStr = PropertyHelper.getProperty("noOfVertices", "10000");
		String noOfEdgesStr = PropertyHelper.getProperty("noOfEdges", "50");

		int noOfVertex = Integer.parseInt(noOfVertexStr);
		int noOfEdges = Integer.parseInt(noOfEdgesStr);

		List<Object> allIds = new ArrayList<Object>(noOfVertex);

		Timer overallTimer = new Timer();		
		Timer verticesTimer = new Timer();
		
		TitanManagement managementSystem = graph.getManagementSystem();
		
		logger.info("Adding " + noOfVertex + " Vertices");
		for (int i = 0; i < noOfVertex; i++) {
			
			Vertex a = graph.addVertex(null);

			String issuer = issuers.get(new Double(Math.random() * issuers.size()).intValue());
			String location = locations.get(new Double(Math.random() * locations.size()).intValue());

			a.setProperty("name", "U" + i + 1);
			a.setProperty("userid", i);
			a.setProperty("location", location);
			a.setProperty("issuer", issuer);
			
			graph.commit();
			
			allIds.add(a.getId());
			
			if (i % 1 == 0){				
				logger.info("Total Vertices : " + i);
				managementSystem.commit();
				managementSystem = graph.getManagementSystem();
			}
		}
		managementSystem.commit();
		
		verticesTimer.end();
		int total = noOfEdges * noOfVertex;

		managementSystem = graph.getManagementSystem();
		logger.info("Adding " + total + " edges");
		Timer edgesTimer = new Timer();
		for (int i = 0; i < total; i++) {			
			
			Vertex user1V = graph.getVertex(allIds.get(new Double(Math.random() * allIds.size()).intValue()));
			Vertex user2V = graph.getVertex(allIds.get(new Double(Math.random() * allIds.size()).intValue()));

			Edge e = graph.addEdge(null, graph.getVertex(user1V), graph.getVertex(user2V), "knows");
			e.setProperty("weight", Math.random());
			e.setProperty("time", new Date());			

			if (i % 1 == 0){
				logger.info("Total Edges : " + i);
				managementSystem.commit();
				managementSystem = graph.getManagementSystem();
			}
		}
		managementSystem.commit();
		
		edgesTimer.end();
		overallTimer.end();

//		System.out.println("Vertices of " + graph);
//		for (Vertex vertex : graph.getVertices()) {
//			System.out.println(vertex);
//		}
//		System.out.println("Edges of " + graph);
//		for (Edge edge : graph.getEdges()) {
//			System.out.println(edge);
//		}
		logger.info(graph.toString());
		logger.info(noOfVertexStr + " Vertices took " + verticesTimer.getTimeTakenSeconds() + "secs to complete.");
		logger.info(total + " total edges took " + edgesTimer.getTimeTakenSeconds() + "secs to complete.");
		logger.info("Overall took " + overallTimer.getTimeTakenSeconds() + "secs to complete.");
		
		graph.shutdown();
		logger.info("Finished.");
	}

	private void sleep(int i) {
		try {
			Thread.sleep(i);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		new MainMemory();
	}

	private List<String> locations = Arrays.asList("London", "Manchester", "Liverpool", "Glasgow", "Dundee",
			"Birmingham");

	private List<String> issuers = Arrays.asList("Tesco", "Sainsbury", "Asda Wal-Mart Stores", "Morrisons",
			"Marks & Spencer", "Boots", "John Lewis", "Waitrose", "Argos", "Co-op", "Currys", "PC World", "B&Q",
			"Somerfield", "Next", "Spar", "Amazon", "Costa", "Starbucks", "BestBuy", "Wickes", "TFL", "National Rail",
			"Pizza Hut", "Local Pub");
}
