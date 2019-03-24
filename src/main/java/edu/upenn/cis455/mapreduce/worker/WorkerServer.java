package edu.upenn.cis455.mapreduce.worker;

import static spark.Spark.*;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.databind.ObjectMapper;

import edu.upenn.cis.stormlite.DistributedCluster;
import edu.upenn.cis.stormlite.TopologyContext;
import edu.upenn.cis.stormlite.distributed.WorkerHelper;
import edu.upenn.cis.stormlite.distributed.WorkerJob;
import edu.upenn.cis.stormlite.routers.StreamRouter;
import edu.upenn.cis.stormlite.tuple.Tuple;
import edu.upenn.cis455.mapreduce.RunJobRoute;
import spark.Spark;

/**
 * Simple listener for worker creation 
 * 
 * @author zives
 *
 */
public class WorkerServer {
	static Logger log = LogManager.getLogger(WorkerServer.class);
	
    static DistributedCluster cluster = new DistributedCluster();
    
    List<TopologyContext> contexts = new ArrayList<>();

	int myPort;
	
	static List<String> topologies = new ArrayList<>();
	
	public WorkerServer(int myPort) throws MalformedURLException {
		
		log.info("Creating server listener at socket " + myPort);
	
		port(myPort);
    	final ObjectMapper om = new ObjectMapper();
        om.enableDefaultTyping(ObjectMapper.DefaultTyping.NON_FINAL);
        Spark.post("/definejob", (req, res) -> {
	        	
	        	WorkerJob workerJob;
				try {
					workerJob = om.readValue(req.body(), WorkerJob.class);
		        	
		        	try {
		        		log.info("Processing job definition request" + workerJob.getConfig().get("job") +
		        				" on machine " + workerJob.getConfig().get("workerIndex"));
						contexts.add(cluster.submitTopology(workerJob.getConfig().get("job"), workerJob.getConfig(), 
								workerJob.getTopology()));

						// Add a new topology
						synchronized (topologies) {
							topologies.add(workerJob.getConfig().get("job"));
						}
					} catch (ClassNotFoundException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
		            return "Job launched";
				} catch (IOException e) {
					e.printStackTrace();
					
					// Internal server error
					res.status(500);
					return e.getMessage();
				} 
	        	
        });
        
        Spark.post("/runjob", new RunJobRoute(cluster));
        
        Spark.post("/pushdata/:stream", (req, res) -> {
				try {
					String stream = req.params(":stream");
					Tuple tuple = om.readValue(req.body(), Tuple.class);
					
					log.debug("Worker received: " + tuple + " for " + stream);
					
					// Find the destination stream and route to it
					StreamRouter router = cluster.getStreamRouter(stream);
					
					if (contexts.isEmpty())
						log.error("No topology context -- were we initialized??");
					
					// Instrumentation for tracking progress
			    	if (!tuple.isEndOfStream())
			    		contexts.get(contexts.size() - 1).incSendOutputs(router.getKey(tuple.getValues()));
			    	
			    	// TODO: handle tuple vs end of stream locally
					
					return "OK";
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					
					res.status(500);
					return e.getMessage();
				}
				
        });

	}
	
	public static void createWorker(Map<String, String> config) {
		if (!config.containsKey("workerList"))
			throw new RuntimeException("Worker spout doesn't have list of worker IP addresses/ports");

		if (!config.containsKey("workerIndex"))
			throw new RuntimeException("Worker spout doesn't know its worker ID");
		else {
			String[] addresses = WorkerHelper.getWorkers(config);
			String myAddress = addresses[Integer.valueOf(config.get("workerIndex"))];

			log.debug("Initializing worker " + myAddress);

			URL url;
			try {
				url = new URL(myAddress);

				new WorkerServer(url.getPort());
			} catch (MalformedURLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	public static void shutdown() {
		synchronized(topologies) {
			for (String topo: topologies)
				cluster.killTopology(topo);
		}
		
    	cluster.shutdown();
	}

	/**
	 * Simple launch for worker server.  Note that you may want to change / replace
	 * most of this.
	 * 
	 * @param args
	 * @throws MalformedURLException
	 */
	public static void main(String args[]) throws MalformedURLException {
		if (args.length < 1) {
			System.out.println("Usage: WorkerServer [port number]");
			System.exit(1);
		}
		
		int myPort = Integer.valueOf(args[0]);
		
		System.out.println("Worker node startup, on port " + myPort);
		
		WorkerServer worker = new WorkerServer(myPort);
		
		// TODO: you may want to adapt parts of edu.upenn.cis.stormlite.mapreduce.TestMapReduce
		// here
	}
}
