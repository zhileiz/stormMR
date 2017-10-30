package edu.upenn.cis455.mapreduce.master;

import static spark.Spark.get;
import static spark.Spark.setPort;

public class MasterServer {

  static final long serialVersionUID = 455555001;
  static final int myPort = 8080;

    public static void main(String[] args) {
		setPort(myPort);
		
		get("/status", (request, response) -> {
            response.type("text/html");
            
            return ("<html><head><title>Master</title></head>\n" +
            		"<body>Hi, I am the master!</body></html>");
		});
	}
}
  
