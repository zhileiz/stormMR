package edu.upenn.cis455.mapreduce.master.routes;

import com.fasterxml.jackson.databind.ObjectMapper;
import edu.upenn.cis.stormlite.Config;
import edu.upenn.cis.stormlite.Topology;
import edu.upenn.cis.stormlite.TopologyBuilder;
import edu.upenn.cis.stormlite.bolt.MapBolt;
import edu.upenn.cis.stormlite.bolt.ReduceBolt;
import edu.upenn.cis.stormlite.distributed.WorkerJob;
import edu.upenn.cis.stormlite.spout.FileSpout;
import edu.upenn.cis.stormlite.tuple.Fields;
import edu.upenn.cis455.mapreduce.job.WordFileSpout;
import edu.upenn.cis455.mapreduce.master.MasterConfig;
import spark.Request;
import spark.Response;
import spark.Route;

import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Arrays;

public class JobRoute implements Route {

    private String FILE_SPOUT = "FILE_SPOUT";
    private String MAP_BOLT = "MAP_BOLT";
    private String REDUCER_BOLT = "REDUCER_BOLT";
    private String PRINT_BOLT = "PRINT_BOLT";

    private MasterConfig config;

    private class JobDesc {
        String className, inputDir, outputDir;
        int numMapper, numReducer;
    }

    public JobRoute(MasterConfig config) {
        this.config = config;
    }

    @Override
    public Object handle(Request request, Response response) {
        JobDesc job = getJobDesc(request);
        String[] workersList = config.getWorkersArray();
        int result = sendToAllWorkers(workersList, job);
        return result + " success out of " + workersList.length + " tried.";
    }

    private int sendToAllWorkers(String[] workersList, JobDesc job) {
        int numSuccess = 0;
        Topology top = createTopologyWith(job);
        for (int i = 0; i < workersList.length; i++) {
            String dest = workersList[i];
            Config config = createConfigWith(job, i, workersList);
            WorkerJob wj = new WorkerJob(top, config);
            try {
                if (postJob(wj, dest).getResponseCode() < 300) {
                    numSuccess++;
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        config.setJobOnHold(true);
        return numSuccess;
    }


    private JobDesc getJobDesc(Request req) {
        JobDesc job = new JobDesc();
//        job.className = req.queryParams("class_name");
//        job.inputDir = req.queryParams("input_directory_path");
//        job.outputDir = req.queryParams("output_directory_path");
        job.className = "edu.upenn.cis455.mapreduce.job.WordCount";
        job.numMapper = 1;
        job.numReducer = 1;
        return job;
    }

    private Config createConfigWith(JobDesc job, int index, String[] workersList) {
        Config config = new Config();
        config.put("job", "ZZJob");
        config.put("workerList", Arrays.toString(workersList));
        config.put("workerIndex", String.valueOf(index));
        config.put("mapClass", job.className);
        config.put("reduceClass", job.className);
        config.put("spoutExecutors", "1");
        config.put("mapExecutors", "1");
        config.put("reduceExecutors", "1");
        return config;
    }

    private Topology createTopologyWith(JobDesc job) {
        FileSpout fileSpout = new WordFileSpout();
        MapBolt mapBolt = new MapBolt();
        ReduceBolt reduceBolt = new ReduceBolt();

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout(FILE_SPOUT, fileSpout, 1);
        builder.setBolt(MAP_BOLT, mapBolt, job.numMapper).shuffleGrouping(FILE_SPOUT);
        builder.setBolt(REDUCER_BOLT, reduceBolt, job.numReducer).fieldsGrouping(MAP_BOLT, new Fields());
        return builder.createTopology();
    }

    private HttpURLConnection postJob(WorkerJob job, String dest) {
        ObjectMapper mapper = new ObjectMapper();
        mapper.enableDefaultTyping(ObjectMapper.DefaultTyping.NON_FINAL);
        String tempDest = "http://" + dest;
        try {
            return postJob(tempDest, "POST", job.getConfig(), "definejob",
                    mapper.writerWithDefaultPrettyPrinter().writeValueAsString(job));
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    private HttpURLConnection postJob(String dest, String reqType, Config config, String job, String parameters) throws IOException {
        URL url = new URL(dest + "/" + job);
        HttpURLConnection conn = (HttpURLConnection)url.openConnection();

        conn.setDoOutput(true);
        conn.setRequestMethod(reqType);
        conn.setRequestProperty("Content-Type", "application/json");
        OutputStream os = conn.getOutputStream();
        byte[] toSend = parameters.getBytes();
        os.write(toSend);
        os.flush();

        return conn;
    }
}
