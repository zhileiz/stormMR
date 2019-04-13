package edu.upenn.cis455.mapreduce.master;

import edu.upenn.cis455.mapreduce.WorkerStatus;

import java.util.ArrayList;
import java.util.List;

public class WorkerRecord {
    protected String ip;
    protected WorkerStatus status;
    protected int keysWritten, keysRead;
    protected List<String> results;

    public WorkerRecord(String ip, String status, String keysRead, String keysWritten) {
        this.ip = ip;
        this.status = parseOrIdle(status);
        this.keysRead = parseOrZero(keysRead);
        this.keysWritten = parseOrZero(keysWritten);
        this.results = new ArrayList<String>();
    }

    private int parseOrZero(String s) {
        try {
            return Integer.parseInt(s);
        } catch (Exception e) {
            System.err.println("[ ⚠️: ] Parsing Error - Failed to parse Keys Num");
            return 0;
        }
    }

    public WorkerStatus parseOrIdle(String s) {
        try {
            return WorkerStatus.valueOf(s);
        } catch (IllegalArgumentException e) {
            System.err.println("[ ⚠️: ] Parsing Error - Failed to parse WorkerStatus");
            return WorkerStatus.IDLE;
        }
    }

    public void addResult(String key, String result) {
        results.add("(key:" + key + ",result:" + result + ")\n");
    }

    public String getAllResults() {
        StringBuilder sb = new StringBuilder();
        for (String s : results) {
            sb.append(s);
        }
        return sb.toString();
    }

    public String getIp() {
        return ip;
    }

    public int getKeysRead() {
        return keysRead;
    }

    public int getKeysWritten() {
        return keysWritten;
    }

    public WorkerStatus getStatus() {
        return status;
    }
}
