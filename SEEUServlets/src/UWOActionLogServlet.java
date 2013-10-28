import javax.servlet.http.*;
import javax.servlet.*;

import java.io.*;
import java.net.*;
import java.sql.*;
import java.util.ArrayList;
import java.lang.Double;
import java.util.Calendar;

public class UWOActionLogServlet extends HttpServlet
{
	static Connection conn = null;
	static String url = "jdbc:mysql://192.168.0.2:3306/";
	static String dbName = "seeu";
	static String driver = "com.mysql.jdbc.Driver";
	static String userName = "root"; 
	static String password = "see";
	static String usertb = "link_click_logs";    
    
 // This method is called by the servlet container just before this servlet
 	 // is put into service.
 	 public void init() throws ServletException {	
 	     // The int parameters can also be retrieved using the servlet context
 		 dbName = getServletConfig().getInitParameter("db_schema");
 		 usertb = getServletConfig().getInitParameter("db_tb"); 		 
 	 }
 	
    public void doPost(HttpServletRequest req, HttpServletResponse resp)
                throws ServletException,IOException
    {
        doGet(req,resp);
    }

	public void doGet(HttpServletRequest req, HttpServletResponse resp)
	               throws ServletException,IOException
	    {		
			String user=req.getParameter("user");
			String query=req.getParameter("query");
			String link=req.getParameter("link");
			String curcat=req.getParameter("curcat");
			String pageid=req.getParameter("pageid");
			String page=req.getParameter("page");
			String rank=req.getParameter("rank");
			String solrparameter=req.getParameter("solrparameter");
			String desc=req.getParameter("description");
			String ip = req.getRemoteAddr();
			
			System.out.println(user + ' ' + query + ' ' + link + ' ' + curcat + ' ' + pageid + ' ' + solrparameter + ' ' + desc + ' ' + ip);
			
			if (user == null)
				user = "";
			if(ip == null)
				ip = "";
			
			String msg = "Error";
			if(query == null || link == null || curcat == null || desc == null || pageid == null || solrparameter == null)
			{				
				msg = "Error";							
			}		
			else
			{	
				//get session ID
				HttpSession session = req.getSession(true);
				String sessionid = session.getId();			
				
				//insert log into db
				try{
					Class.forName(driver).newInstance();
					conn = DriverManager.getConnection(url+dbName,userName,password);				
								
					//insert into tb
					PreparedStatement pStmt = conn.prepareStatement("insert into "+usertb+"(user, sessionid, time, curcat, pageid, link, page, rank, query, solrparameter, ip, description) values(?,?,?,?,?,?,?,?,?,?,?,?)");
					pStmt.setString(1, user);
					pStmt.setString(2, sessionid);
					java.sql.Timestamp ourJavaTimestampObject = new java.sql.Timestamp(Calendar.getInstance().getTime().getTime());
				    pStmt.setTimestamp(3, ourJavaTimestampObject);			    
				    pStmt.setString(4, curcat);
				    pStmt.setString(5, pageid);
				    pStmt.setString(6, link);
				    pStmt.setInt(7, Integer.parseInt(page));
				    pStmt.setInt(8, Integer.parseInt(rank));
				    pStmt.setString(9, query);
				    pStmt.setString(10, solrparameter);
				    pStmt.setString(11, ip);
				    pStmt.setString(12, desc);
	
				    System.out.println(pStmt.toString());
				    			    
				    pStmt.executeUpdate();
																		
					conn.close();
					
					msg = "Done";
				} catch (Exception e) {
					System.out.println("database error");
					e.printStackTrace();
				}
			}		
			
			java.io.PrintWriter pw=resp.getWriter();				
			pw.write(msg);
			pw.close();
			
	    }	  
}