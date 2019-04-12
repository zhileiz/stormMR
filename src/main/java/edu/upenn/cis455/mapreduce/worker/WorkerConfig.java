package edu.upenn.cis455.mapreduce.worker;

import edu.upenn.cis455.mapreduce.RunJobRoute;
import edu.upenn.cis455.mapreduce.worker.routes.DefineJobRoute;
import edu.upenn.cis455.mapreduce.worker.routes.PushDataRoute;

import java.io.IOException;
import java.net.MalformedURLException;

import static spark.Spark.port;
import static spark.Spark.post;

public class WorkerConfig {

    private int myPort;

    public WorkerConfig(int port) {
        myPort = port;
        setUpPort(port);
        setUpRoutes();
        System.out.println("Creating server listener at socket " + port);
        notifyMaster();
    }

    private void setUpRoutes() {
        post("/definejob", new DefineJobRoute());
        post("/runjob", new RunJobRoute(WorkerCenter.getCluster()));
        post("/pushdata/:stream", new PushDataRoute());
    }

    private void setUpPort(int portNum) {
        port(portNum);
    }

    public void notifyMaster() {
        try {
            WorkerCenter.getInstance().initialUpdateMaster(myPort);
        } catch (MalformedURLException e) {
            System.err.println("[ ❌🔗: ] Failed To Notify Master Due to MalformedURLException");
        } catch (IOException e) {
            e.printStackTrace();
            System.err.println("[ ❌🎹: ] Failed To Notify Master Due to IOException");
        }
    }



}
