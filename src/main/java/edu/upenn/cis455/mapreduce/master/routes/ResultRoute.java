package edu.upenn.cis455.mapreduce.master.routes;

import edu.upenn.cis455.mapreduce.master.MasterConfig;
import spark.Request;
import spark.Response;
import spark.Route;

public class ResultRoute implements Route {
    private MasterConfig master;

    public ResultRoute(MasterConfig master) {
        this.master = master;
    }

    @Override
    public Object handle(Request req, Response res) {
        String ip = req.ip() + ":" + req.queryParams("port");
        String key = req.queryParams("key");
        String result = req.queryParams("result");
        System.out.println(ip + key + result);
        if (key != null && result != null) {
            master.addResult(ip, key, result);
        }
        return "Good";
    }

}