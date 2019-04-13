package edu.upenn.cis455.mapreduce.master;

import edu.upenn.cis455.mapreduce.WorkerStatus;
import edu.upenn.cis455.mapreduce.master.routes.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static spark.Spark.*;

public class MasterConfig {

    private HashMap<String, WorkerRecord> workerRecords;
    private Boolean jobOnHold;

    private HashMap<String, List<String>> reduceResults;

    public MasterConfig(int portNum) {
        workerRecords = new HashMap<>();
        reduceResults = new HashMap<>();
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
        post("/addResult", new ResultRoute(this));
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

    public void addResult(String ipAndPort, String key, String value) {
        String s = "(key:" + key + ", value:" + value + ")\n";
        if (!reduceResults.containsKey(ipAndPort)) {
            List<String> results = new ArrayList<>();
            results.add(s);
            reduceResults.put(ipAndPort, results);
        } else {
            reduceResults.get(ipAndPort).add(s);
        }
    }

    public String getAllResultsFor(String key) {
        List<String> values = reduceResults.get(key);
        if (values == null) { return ""; }
        StringBuilder sb = new StringBuilder();
        for (String value : values) {
            sb.append(value);
        }
        return sb.toString();
    }

    public synchronized void setJobOnHold(Boolean jobOnHold) {
        this.jobOnHold = jobOnHold;
    }

    public Boolean getJobOnHold() {
        return jobOnHold;
    }
}
