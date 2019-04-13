package edu.upenn.cis455.mapreduce;

import spark.Request;
import spark.Response;
import spark.Route;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import edu.upenn.cis.stormlite.Config;
import edu.upenn.cis.stormlite.DistributedCluster;
import edu.upenn.cis.stormlite.TopologyBuilder;
import edu.upenn.cis455.mapreduce.worker.WorkerServer;

public class RunJobRoute implements Route {
	static Logger log = LogManager.getLogger(RunJobRoute.class);
	DistributedCluster cluster;
	
	public RunJobRoute(DistributedCluster cluster) {
		this.cluster = cluster;
	}

	@Override
	public Object handle(Request request, Response response) throws Exception {
		System.out.println("Starting Job Here");

		cluster.startTopology();
		
		return "Started";
	}

}
