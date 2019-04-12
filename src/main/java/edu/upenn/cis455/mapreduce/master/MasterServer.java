package edu.upenn.cis455.mapreduce.master;

import static spark.Spark.*;

public class MasterServer {

  static final long serialVersionUID = 455555001;
  static final int myPort = 8089;
  
  public static void registerStatusPage() { }

  /**
   * The mainline for launching a MapReduce Master.  This should
   * handle at least the status and workerstatus routes, and optionally
   * initialize a worker as well.
   * 
   * @param args
   */
    public static void main(String[] args) {

        new MasterConfig(myPort);
		
		// TODO: you may want to adapt parts of edu.upenn.cis.stormlite.mapreduce.TestMapReduce
		// here
		
		// TODO: route handler for /workerstatus reports from the workers
	}
}
  
