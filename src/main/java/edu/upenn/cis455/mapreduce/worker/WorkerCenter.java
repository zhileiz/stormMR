package edu.upenn.cis455.mapreduce.worker;

import edu.upenn.cis.stormlite.DistributedCluster;
import edu.upenn.cis.stormlite.TopologyContext;
import edu.upenn.cis.stormlite.distributed.WorkerJob;
import edu.upenn.cis455.mapreduce.WorkerStatus;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

public class WorkerCenter {

    private static final String MASTER_LOCATION = "http://127.0.0.1:8089";

    private static WorkerCenter instance;

    public static WorkerCenter getInstance() {
        if (instance == null) {
            instance = new WorkerCenter();
        }
        return instance;
    }

    static List<String> topologies = new ArrayList<>();
    static DistributedCluster cluster = new DistributedCluster();

    List<TopologyContext> contexts;
    Integer portNum = 0, keysRead = 0, keysWritten = 0;
    WorkerStatus status;

    private WorkerCenter() {
        contexts = new ArrayList<>();
        status = WorkerStatus.IDLE;
    }

    public void addContext(TopologyContext context) {
        synchronized (contexts) {
            contexts.add(context);
        }
    }

    public void addTopology(String topoId) {
        synchronized (topologies) {
            topologies.add(topoId);
        }
    }

    public TopologyContext submitTopologyToCluster(WorkerJob job) throws ClassNotFoundException {
        return cluster.submitTopology((job.getConfig().get("job")), job.getConfig(),
                job.getTopology());
    }

    public static DistributedCluster getCluster() { return cluster; }
    public List<TopologyContext> getContexts() { return contexts; }

    public static void shutDown() {
        synchronized(topologies) {
            for (String topo: topologies) {
                cluster.killTopology(topo);
            }
        }
        cluster.shutdown();
    }

    public synchronized void updateMaster() throws MalformedURLException, IOException {
        StringBuilder sb = new StringBuilder();
        sb.append(MASTER_LOCATION + "/workerStatus?");
        sb.append("port=" + String.valueOf(this.portNum));
        sb.append("&status=" + status.toString());
        sb.append("&keysRead=" + keysRead);
        sb.append("&keysWritten=" + keysWritten);
        System.out.println(sb.toString());
        URL url = new URL(sb.toString());
        HttpURLConnection conn = (HttpURLConnection)url.openConnection();
        conn.setRequestMethod("POST");
        conn.connect();
        printResponse(conn);
    }

    public void updateKeysRead() {
        synchronized (keysRead) {
            keysRead++;
        }
        try {
            updateMaster();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void updateKeysWritten() {
        synchronized (keysWritten) {
            keysWritten++;
        }
        try {
            updateMaster();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void updateWorkerStatus(WorkerStatus st) {
        if (st == status) { return; }
        synchronized (status) {
            this.status = st;
        }
        try {
            updateMaster();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void sendResultToMaster(String key, String value) throws MalformedURLException, IOException {
        StringBuilder sb = new StringBuilder();
        sb.append(MASTER_LOCATION + "/addResult?");
        sb.append("port=" + portNum);
        sb.append("&key=" + key);
        sb.append("&result=" + value);
        URL url = new URL(sb.toString());
        HttpURLConnection conn = (HttpURLConnection)url.openConnection();
        conn.setRequestMethod("POST");
        conn.connect();
        printResponse(conn);
    }

    public void initialUpdateMaster(int portNum) throws MalformedURLException, IOException {
        this.portNum = portNum;
        updateMaster();
    }

    private void printResponse(HttpURLConnection httpURLConnection) throws IOException {
        StringBuilder builder = new StringBuilder();
        builder.append(httpURLConnection.getResponseCode())
                .append(" ")
                .append(httpURLConnection.getResponseMessage())
                .append("\n");
        System.out.println(builder);
    }
}
