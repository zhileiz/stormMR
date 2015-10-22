package edu.upenn.cis455.mapreduce.worker;

import java.io.*;
import javax.servlet.*;
import javax.servlet.http.*;

public class WorkerServlet extends HttpServlet {

  static final long serialVersionUID = 455555002;

  public void doGet(HttpServletRequest request, HttpServletResponse response) 
       throws java.io.IOException
  {
    response.setContentType("text/html");
    PrintWriter out = response.getWriter();
    out.println("<html><head><title>Worker</title></head>");
    out.println("<body>Hi, I am the worker!</body></html>");
  }
}
  
