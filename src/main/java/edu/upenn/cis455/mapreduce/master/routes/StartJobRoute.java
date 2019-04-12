package edu.upenn.cis455.mapreduce.master.routes;

import edu.upenn.cis455.mapreduce.master.MasterConfig;
import spark.Request;
import spark.Response;
import spark.Route;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;

public class StartJobRoute implements Route {

    MasterConfig master;

    public StartJobRoute(MasterConfig master) {
        this.master = master;
    }

    @Override
    public Object handle(Request req, Response res) {
        String[] workerLinks = master.getWorkersArray();
        master.setJobOnHold(false);
        try {
            for (String worker : workerLinks) {
                sendRequest(worker);
            }
            return "Job Sent";
        } catch (Exception e) {
            return e.getMessage();
        }
    }

    private void sendRequest(String worker) throws MalformedURLException, IOException {
        String dest = "http://" + worker + "/runjob";
        URL url = new URL(dest.toString());
        HttpURLConnection conn = (HttpURLConnection)url.openConnection();
        conn.setRequestMethod("POST");
        conn.connect();
        printResponse(conn);
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
