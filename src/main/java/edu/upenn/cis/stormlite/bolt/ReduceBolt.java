package edu.upenn.cis.stormlite.bolt;

import java.util.*;

import edu.upenn.cis455.mapreduce.Context;
import edu.upenn.cis455.mapreduce.WorkerStatus;
import edu.upenn.cis455.mapreduce.worker.WorkerCenter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import edu.upenn.cis.stormlite.OutputFieldsDeclarer;
import edu.upenn.cis.stormlite.TopologyContext;
import edu.upenn.cis.stormlite.distributed.ConsensusTracker;
import edu.upenn.cis.stormlite.routers.StreamRouter;
import edu.upenn.cis.stormlite.tuple.Fields;
import edu.upenn.cis.stormlite.tuple.Tuple;
import edu.upenn.cis455.mapreduce.Job;

/**
 * A simple adapter that takes a MapReduce "Job" and calls the "reduce"
 * on a per-tuple basis
 * 
 */
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

public class ReduceBolt implements IRichBolt {
	static Logger log = LogManager.getLogger(ReduceBolt.class);

	
	Job reduceJob;

	/**
	 * This object can help determine when we have
	 * reached enough votes for EOS
	 */
	ConsensusTracker votesForEos;

	/**
     * To make it easier to debug: we have a unique ID for each
     * instance of the WordCounter, aka each "executor"
     */
    String executorId = UUID.randomUUID().toString();
    
	Fields schema = new Fields("key", "value");
	
	boolean sentEos = false;
	
	/**
	 * Buffer for state, by key
	 */
	Map<String, List<String>> stateByKey = new HashMap<>();

	/**
     * This is where we send our output stream
     */
    private OutputCollector collector;
    
    private TopologyContext context;

    List<Tuple> tuples = new ArrayList<>();
    
    int neededVotesToComplete = 0;
    
    public ReduceBolt() {
    }
    
    /**
     * Initialization, just saves the output stream destination
     */
    @Override
    public void prepare(Map<String,String> stormConf, 
    		TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.context = context;

        if (!stormConf.containsKey("reduceClass"))
        	throw new RuntimeException("Mapper class is not specified as a config option");
        else {
        	String mapperClass = stormConf.get("reduceClass");
        	
        	try {
				reduceJob = (Job)Class.forName(mapperClass).newInstance();
			} catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				throw new RuntimeException("Unable to instantiate the class " + mapperClass);
			}
        }
        if (!stormConf.containsKey("mapExecutors")) {
        	throw new RuntimeException("Reducer class doesn't know how many map bolt executors");
        }

		int numWorkers = stormConf.get("workerList").split(",").length;
        int numMaps = Integer.parseInt(stormConf.getOrDefault("mapExecutors", "1"));
        this.neededVotesToComplete = numWorkers * numMaps;
        this.votesForEos = new ConsensusTracker(neededVotesToComplete);
    }

    /**
     * Process a tuple received from the stream, buffering by key
     * until we hit end of stream
     */
    @Override
    public synchronized boolean execute(Tuple input) {
    	if (sentEos) {
	        if (!input.isEndOfStream())
	        	throw new RuntimeException("We received data after we thought the stream had ended!");
    		// Already done!
	        return false;
		} else if (input.isEndOfStream()) {
			
    		log.debug("Processing EOS from " + input.getSourceExecutor());
    		// the associated key, and output all state
			System.out.println("[👌] Received EOS");
			if (votesForEos.voteForEos(input.getSourceExecutor())) {
				Context outputContext = new Context() {
					@Override
					public void write(String key, String value, String sourceExecutor) {
						try {
							WorkerCenter.getInstance().sendResultToMaster(key, value);
						} catch (Exception e) {
							e.printStackTrace();
						}
						System.out.println("[👏 RESULT: ] " + key + " : " + value);
					}
				};
				System.out.println(stateByKey.toString());
				for (String key : stateByKey.keySet()) {
					WorkerCenter.getInstance().updateKeysWritten();
					reduceJob.reduce(key, stateByKey.get(key).iterator(), outputContext, getExecutorId());
				}
				WorkerCenter.getInstance().updateWorkerStatus(WorkerStatus.IDLE);
			}

    	} else {
			WorkerCenter.getInstance().updateWorkerStatus(WorkerStatus.REDUCING);
    		String key = input.getStringByField("key");
    		String value = input.getStringByField("value");
			List<String> values;
    		if (stateByKey.containsKey(key)) {
				values = stateByKey.get(key);
			} else {
    			values = new ArrayList<>();
			}
			values.add(value);
			stateByKey.put(key, values);
    		log.debug("Processing " + input.toString() + " from " + input.getSourceExecutor());
    		
    	}        
    	return true;
    }

    /**
     * Shutdown, just frees memory
     */
    @Override
    public void cleanup() {
    }

    /**
     * Lets the downstream operators know our schema
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(schema);
    }

    /**
     * Used for debug purposes, shows our exeuctor/operator's unique ID
     */
	@Override
	public String getExecutorId() {
		return executorId;
	}

	/**
	 * Called during topology setup, sets the router to the next
	 * bolt
	 */
	@Override
	public void setRouter(StreamRouter router) {
		this.collector.setRouter(router);
	}

	/**
	 * The fields (schema) of our output stream
	 */
	@Override
	public Fields getSchema() {
		return schema;
	}

}
