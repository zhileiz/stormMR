package edu.upenn.cis455.mapreduce.worker.routes;

import com.fasterxml.jackson.databind.ObjectMapper;
import edu.upenn.cis.stormlite.TopologyContext;
import edu.upenn.cis.stormlite.distributed.WorkerJob;
import edu.upenn.cis455.mapreduce.worker.WorkerCenter;
import spark.Request;
import spark.Response;
import spark.Route;

import java.io.IOException;

public class DefineJobRoute implements Route {

    @Override
    public Object handle(Request req, Response res) {
        final ObjectMapper om = new ObjectMapper();
        om.enableDefaultTyping(ObjectMapper.DefaultTyping.NON_FINAL);
        WorkerJob workerJob;
        try {
            workerJob = om.readValue(req.body(), WorkerJob.class);
            try {
                System.out.println("Processing job definition request" + workerJob.getConfig().get("job") +
                        " on machine " + workerJob.getConfig().get("workerIndex"));
                TopologyContext context = WorkerCenter.getInstance().submitTopologyToCluster(workerJob);
                WorkerCenter.getInstance().addContext(context);
                WorkerCenter.getInstance().addTopology(workerJob.getConfig().get("job"));
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }
            return "Job launched";
        } catch (IOException e) {
            e.printStackTrace();
            // Internal server error
            res.status(500);
            return e.getMessage();
        }

    }
}
