package edu.upenn.cis455.mapreduce.worker;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import edu.upenn.cis.stormlite.distributed.WorkerHelper;

/**
 * Simple listener for worker creation 
 * 
 * @author zives
 *
 */
public class WorkerServer {
	static Logger log = LogManager.getLogger(WorkerServer.class);
	
	public WorkerServer(int myPort) { new WorkerConfig(myPort); }

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

	/**
	 *  Implemented
	 */
	public static void shutdown() { WorkerCenter.shutDown(); }

	public static void createWorker(Map<String, String> config) {
		if (!config.containsKey("workerList")) {
			throw new RuntimeException("Worker spout doesn't have list of worker IP addresses/ports");
		}

		if (!config.containsKey("workerIndex")) {
			throw new RuntimeException("Worker spout doesn't know its worker ID");
		} else {
			String[] addresses = WorkerHelper.getWorkers(config);
			System.out.println(config.get("workerIndex"));
			String myAddress = addresses[Integer.valueOf(config.get("workerIndex"))];
			log.debug("Initializing worker " + myAddress);
			URL url;
			try {
				url = new URL(myAddress);
				new WorkerServer(url.getPort());
			} catch (MalformedURLException e) {
				e.printStackTrace();
			}
		}
	}
}
