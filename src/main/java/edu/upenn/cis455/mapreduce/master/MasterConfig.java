package edu.upenn.cis455.mapreduce.master;

import edu.upenn.cis455.mapreduce.WorkerStatus;
import edu.upenn.cis455.mapreduce.master.routes.JobRoute;
import edu.upenn.cis455.mapreduce.master.routes.StatusRoute;
import edu.upenn.cis455.mapreduce.master.routes.WorkerStatusRoute;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static spark.Spark.*;

public class MasterConfig {

    private HashMap<String, WorkerRecord> workerRecords;

    public MasterConfig(int portNum) {
        workerRecords = new HashMap<>();
        setUpPort(portNum);
        setUpRoutes();
        System.out.println("Master node startup");
    }

    private void setUpPort(int portNum) {
        port(portNum);
    }

    private void setUpRoutes() {
        get("/status", new StatusRoute(this));
        post("/workerStatus", new WorkerStatusRoute(this));
        post("/job", new JobRoute());
    }

    public List<WorkerRecord> getWorkerRecords() {
        List<WorkerRecord> records = new ArrayList<WorkerRecord>();
        for (Object value : workerRecords.values()) {
            records.add((WorkerRecord) value);
        }
        return records;
    }

    public void refreshWorkerStatus(WorkerRecord record) {
        synchronized (workerRecords) {
            workerRecords.put(record.ip, record);
        }
    }
}
