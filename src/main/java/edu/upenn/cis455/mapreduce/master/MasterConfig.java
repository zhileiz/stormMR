package edu.upenn.cis455.mapreduce.master;

import edu.upenn.cis455.mapreduce.WorkerStatus;
import edu.upenn.cis455.mapreduce.master.routes.JobRoute;
import edu.upenn.cis455.mapreduce.master.routes.StartJobRoute;
import edu.upenn.cis455.mapreduce.master.routes.StatusRoute;
import edu.upenn.cis455.mapreduce.master.routes.WorkerStatusRoute;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static spark.Spark.*;

public class MasterConfig {

    private HashMap<String, WorkerRecord> workerRecords;
    private Boolean jobOnHold;

    public MasterConfig(int portNum) {
        workerRecords = new HashMap<>();
        setUpPort(portNum);
        setUpRoutes();
        this.jobOnHold = false;
        System.out.println("Master node startup");
    }

    private void setUpPort(int portNum) {
        port(portNum);
    }

    private void setUpRoutes() {
        get("/status", new StatusRoute(this));
        post("/workerStatus", new WorkerStatusRoute(this));
        post("/job", new JobRoute(this));
        get("/startAllWorkers", new StartJobRoute(this));
    }

    public List<WorkerRecord> getWorkerRecords() {
        List<WorkerRecord> records = new ArrayList<WorkerRecord>();
        for (Object value : workerRecords.values()) {
            records.add((WorkerRecord) value);
        }
        return records;
    }

    public String[] getWorkersArray() {
        String[] result = new String[workerRecords.size()];
        int i = 0;
        for (String key : workerRecords.keySet()) {
            System.out.println(key);
            result[i] = key;
            i++;
        }
        return result;
    }

    public void refreshWorkerStatus(WorkerRecord record) {
        synchronized (workerRecords) {
            workerRecords.put(record.ip, record);
        }
    }

    public synchronized void setJobOnHold(Boolean jobOnHold) {
        this.jobOnHold = jobOnHold;
    }

    public Boolean getJobOnHold() {
        return jobOnHold;
    }
}
