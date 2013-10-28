import javax.servlet.http.*;
import javax.servlet.*;
import java.io.*;
import java.net.*;
import java.sql.*;
import java.util.ArrayList;
import java.lang.Double;
import java.util.Calendar;
import java.util.Collections;
import java.util.Enumeration;

public class JsonUserCommentServlet extends HttpServlet
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
    
	 // This method is called by the servlet container just before this servlet
	 // is put into service.
	 public void init() throws ServletException {	
	     // The int parameters can also be retrieved using the servlet context
		 dbName = getServletConfig().getInitParameter("db_schema");
		 tb = getServletConfig().getInitParameter("db_tb");		 
		 dest = getServletConfig().getInitParameter("redirect_dest");
	 }
	 
	public void doGet(HttpServletRequest req, HttpServletResponse resp)
               throws ServletException,IOException
    {		
		String name=req.getParameter("name");
		String email=req.getParameter("email");
		String comment=req.getParameter("comment");				
		String desc=req.getParameter("description");
		String ip = req.getRemoteAddr();
		
		//get session ID
		HttpSession session = req.getSession(true);
		String sessionid = session.getId();	
		
		//get http header
		ArrayList<String> head_lst = new ArrayList<String>();						
		Enumeration<String> enu = (Enumeration<String>)req.getHeaderNames();
		boolean first = true;
		while(enu.hasMoreElements())
		{
			String head_name = (String)enu.nextElement();
			String head_value = req.getHeader(head_name);
			head_lst.add("<" + head_name + ">" + head_value + "</>");				
		}
		//sort it
		Collections.sort(head_lst);
		
		String headers = "";
		for(int i=0;i<head_lst.size();i++)
		{
			if(first == true)
			{
				headers = head_lst.get(i);				
				first = false;
			}
			else
				headers += "<<>><" + head_lst.get(i);
		}
		
		System.out.println("receive user comment" + name + " " + email + " " + comment);
		
		if(name == null)
			name = "";
		if(email == null)
			email = "";
		if(desc == null)
			desc = "";
		
		String msg = "ret={\"url\":\"-1\"}";
		
		try{
			Class.forName(driver).newInstance();
			conn = DriverManager.getConnection(url+dbName,userName,password);				
				
			//insert into tb
			PreparedStatement pStmt = conn.prepareStatement("insert into "+tb+"(sessionid, time, name, email, comment, ip, httpheaders, description) values(?,?,?,?,?,?,?,?)");
			pStmt.setString(1, sessionid);
			java.sql.Timestamp timestamp = new java.sql.Timestamp(Calendar.getInstance().getTime().getTime());
		    pStmt.setTimestamp(2, timestamp);
			pStmt.setString(3, name);
			pStmt.setString(4, email);
			pStmt.setString(5, comment);						    		   
		    pStmt.setString(6, ip);
		    pStmt.setString(7, headers);
		    pStmt.setString(8, desc);		    
			
		    System.out.println(pStmt.toString());
		    
		    pStmt.executeUpdate();
																
			conn.close();
			
			//resp.sendRedirect(dest);
			msg = "ret={\"url\":\""+dest+"\"}";
		} catch (Exception e) {
			System.out.println("database error");
			e.printStackTrace();
			msg = "ret={\"url\":\"-1\"}";			
		}
		
		resp.addHeader("Content-Type", "text/javascript");
		resp.addHeader("Access-Control-Allow-Origin", "*");	
		
		java.io.PrintWriter pw=resp.getWriter();
		pw.write(msg);								
		pw.close();
		
    }
    public void doPost(HttpServletRequest req, HttpServletResponse resp)
                throws ServletException,IOException
    {
        doGet(req,resp);
    }	   
}