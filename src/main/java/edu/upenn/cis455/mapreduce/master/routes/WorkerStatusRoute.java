package edu.upenn.cis455.mapreduce.master.routes;

import edu.upenn.cis455.mapreduce.master.MasterConfig;
import edu.upenn.cis455.mapreduce.master.WorkerRecord;
import spark.Request;
import spark.Response;
import spark.Route;

public class WorkerStatusRoute implements Route {
    private MasterConfig master;

    public WorkerStatusRoute(MasterConfig master) {
        this.master = master;
    }

    @Override
    public Object handle(Request req, Response res) {
        WorkerRecord record = getRecordFromReq(req);
        master.refreshWorkerStatus(record);
        return "received.";
    }

    private WorkerRecord getRecordFromReq(Request req) {
        String ip = req.ip() + ": " + req.queryParams("port");
        String status = req.queryParams("status");
        String keysRead = req.queryParams("keysRead");
        String keysWritten = req.queryParams("keysWritten");
        return new WorkerRecord(ip, status, keysRead, keysWritten);
    }
}
