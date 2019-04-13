package edu.upenn.cis455.mapreduce.worker.routes;

import com.fasterxml.jackson.databind.ObjectMapper;
import edu.upenn.cis.stormlite.TopologyContext;
import edu.upenn.cis.stormlite.routers.StreamRouter;
import edu.upenn.cis.stormlite.tuple.Tuple;
import edu.upenn.cis455.mapreduce.worker.WorkerCenter;
import spark.Request;
import spark.Response;
import spark.Route;

import java.io.IOException;

public class PushDataRoute implements Route {

    @Override
    public Object handle(Request req, Response res) {
        final ObjectMapper om = new ObjectMapper();
        om.enableDefaultTyping(ObjectMapper.DefaultTyping.NON_FINAL);
        try {
            String stream = req.params(":stream");
//            System.out.println("Worker received: " + req.body());
            Tuple tuple = om.readValue(req.body(), Tuple.class);
            System.out.println("Worker received: " + tuple + " from remote");

            // Find the destination stream and route to it
            WorkerCenter ws = WorkerCenter.getInstance();
            StreamRouter router = WorkerCenter.getCluster().getStreamRouter(stream);

            if (ws.getContexts().isEmpty()) {
                System.out.println(("No topology context -- were we initialized??"));
            }

            TopologyContext ourContext = ws.getContexts().get(ws.getContexts().size() - 1);

            // Instrumentation for tracking progress
            if (!tuple.isEndOfStream()) {
                ourContext.incSendOutputs(router.getKey(tuple.getValues()));
            }

            if (!tuple.isEndOfStream()) {
                router.executeLocally(tuple, ourContext, tuple.getSourceExecutor());
            } else {
                router.executeEndOfStreamLocally(ourContext, tuple.getSourceExecutor());
            }

            return "OK";
        } catch (IOException e) {
            e.printStackTrace();

            res.status(500);
            return e.getMessage();
        }
    }
}
