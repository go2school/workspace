import javax.servlet.http.*;
import javax.servlet.*;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;

import java.io.*;
import java.net.*;
import java.sql.*;
import java.util.ArrayList;
import java.lang.Double;
import java.util.Calendar;
import java.util.Collections;
import java.util.Enumeration;

public class TermAutoComleteServlet extends HttpServlet
{
	static Connection conn = null;
	static String url = "jdbc:mysql://192.168.0.2:3306/";
	static String dbName = "seeu";
	static String driver = "com.mysql.jdbc.Driver";
	static String userName = "root"; 
	static String password = "see";
	static String tb = "comments";
	static String destname = "SEEU";
    static String dest = "http://kdd.csd.uwo.ca:88/seeu/index.html";
    static String solrURL = "";
    
	 // This method is called by the servlet container just before this servlet
	 // is put into service.
	 public void init() throws ServletException {	
	     // The int parameters can also be retrieved using the servlet context		 
		 solrURL = getServletConfig().getInitParameter("solrURL");		 
	 }
	 
	public void doGet(HttpServletRequest req, HttpServletResponse resp)
               throws ServletException,IOException
    {		
		String termsfl =req.getParameter("terms.fl");		
		String termsprefix = req.getParameter("terms.prefix");
			
		//query solr
		HttpClient httpclient = new DefaultHttpClient();
		HttpGet httpget = new HttpGet(solrURL + "?terms.fl" + termsfl + "&terms.prefix" + termsprefix + "&wt=json&omitHeader=true");
		HttpResponse response = httpclient.execute(httpget);
		HttpEntity entity = response.getEntity();
		String content = "";
		if (entity != null) {
			InputStream instream = entity.getContent();
			BufferedReader br = new BufferedReader(new InputStreamReader(instream));
			String tmp_str = "";				
			while ((tmp_str = br.readLine()) != null) {				
				content += tmp_str;						
			}							
		}
		System.out.println(content);
		//return results
		java.io.PrintWriter pw=resp.getWriter();
		pw.write(content);								
		pw.close();	
    }
    public void doPost(HttpServletRequest req, HttpServletResponse resp)
                throws ServletException,IOException
    {
        doGet(req,resp);
    }	   
}