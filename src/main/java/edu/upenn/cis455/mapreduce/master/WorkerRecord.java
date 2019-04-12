package edu.upenn.cis455.mapreduce.master;

import edu.upenn.cis455.mapreduce.WorkerStatus;

public class WorkerRecord {
    protected String ip;
    protected WorkerStatus status;
    protected int keysWritten, keysRead;

    public WorkerRecord(String ip, String status, String keysRead, String keysWritten) {
        this.ip = ip;
        this.status = parseOrIdle(status);
        this.keysRead = parseOrZero(keysRead);
        this.keysWritten = parseOrZero(keysWritten);
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
