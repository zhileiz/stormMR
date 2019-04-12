package edu.upenn.cis455.mapreduce;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import edu.upenn.cis.stormlite.Config;
import edu.upenn.cis.stormlite.Topology;
import edu.upenn.cis.stormlite.TopologyBuilder;
import edu.upenn.cis.stormlite.bolt.MapBolt;
import edu.upenn.cis.stormlite.bolt.ReduceBolt;
import edu.upenn.cis.stormlite.distributed.WorkerHelper;
import edu.upenn.cis.stormlite.distributed.WorkerJob;
import edu.upenn.cis.stormlite.spout.FileSpout;
import edu.upenn.cis.stormlite.tuple.Fields;
import edu.upenn.cis455.mapreduce.master.MasterServer;
import edu.upenn.cis455.mapreduce.worker.WorkerServer;

/**
 * Simple word counter test case, largely derived from
 * https://github.com/apache/storm/tree/master/examples/storm-mongodb-examples
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
public class Driver {
    static Logger log = LogManager.getLogger(Driver.class);

    private static final String WORD_SPOUT = "WORD_SPOUT";
    private static final String MAP_BOLT = "MAP_BOLT";
    private static final String REDUCE_BOLT = "REDUCE_BOLT";
    private static final String PRINT_BOLT = "PRINT_BOLT";

    static void createSampleMapReduce(Config config) {
        // Job name
        config.put("job", "MyJob1");

        // IP:port for /workerstatus to be sent
        config.put("master", "127.0.0.1:8088");

        // Class with map function
        config.put("mapClass", "edu.upenn.cis.stormlite.mapreduce.GroupWords");
        // Class with reduce function
        config.put("reduceClass", "edu.upenn.cis.stormlite.mapreduce.GroupWords");

        // Numbers of executors (per node)
        config.put("spoutExecutors", "1");
        config.put("mapExecutors", "1");
        config.put("reduceExecutors", "1");

    }

    /**
     * Command line parameters:
     *
     * Argument 0: worker index in the workerList (0, 1, ...)
     * Argument 1: non-empty parameter specifying that this is the requestor (so we know to send the JSON)
     *
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        Config config = new Config();
        config.put("workerList", "[127.0.0.1:8088,127.0.0.1:8001,127.0.0.1:8002]");


    }

    static HttpURLConnection sendJob(String dest, String reqType, Config config, String job, String parameters) throws IOException {
        URL url = new URL(dest + "/" + job);

        log.info("Sending request to " + url.toString());

        HttpURLConnection conn = (HttpURLConnection)url.openConnection();
        conn.setDoOutput(true);
        conn.setRequestMethod(reqType);

        if (reqType.equals("POST")) {
            conn.setRequestProperty("Content-Type", "application/json");

            OutputStream os = conn.getOutputStream();
            byte[] toSend = parameters.getBytes();
            os.write(toSend);
            os.flush();
        } else
            conn.getOutputStream();

        return conn;
    }
}
