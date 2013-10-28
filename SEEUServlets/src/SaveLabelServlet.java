import javax.servlet.http.*;
import javax.servlet.*;
import java.io.*;
import java.net.*;
import java.sql.*;
import java.util.ArrayList;
import java.lang.Double;
import java.util.Calendar;

public class SaveLabelServlet extends HttpServlet
{
	static Connection conn = null;
	static String url = "jdbc:mysql://192.168.0.2:3306/";
	static String dbName = "see";
	static String driver = "com.mysql.jdbc.Driver";
	static String userName = "root"; 
	static String password = "see";
	static double default_threshold=0.5;
    
	public void doGet(HttpServletRequest req, HttpServletResponse resp)
               throws ServletException,IOException
    {		
		HttpSession session = req.getSession(true);
		if (session.getAttribute("name")!=null)
		{
			String name = (String) session.getAttribute("name");
			System.out.println(name);
		}
		else
			return ;
			
		
    	/*
		int cat_id=getCurrentCategory(req);
		String query=getCurrentQuery(req);
		double threshold=getThreshold(req,Integer.toString(cat_id));
		String ip=req.getRemoteAddr();
		String url1=req.getParameter("url");
		String rank=req.getParameter("rank");
		int page=getCurrentPage(req);
		System.out.println(rank);
		System.out.println(url);
		try{
			Class.forName(driver).newInstance();
			conn = DriverManager.getConnection(url+dbName,userName,password);
			PreparedStatement pStmt = conn.prepareStatement("insert into log(ip,query,category,threshold,url,page,time,rank) values('"+ip+"','"+query+"',"+cat_id+","+threshold+",'"+url1+"',"+page+",?,"+rank+")");
			//System.out.println("insert into log(ip,query,category,threshold,url,page,time,rank) values('"+ip+"','"+query+"',"+threshold+","+cat_id+",'"+url1+"',"+page+",?,"+rank+")");
			Calendar calendar = Calendar.getInstance();
			java.sql.Timestamp ourJavaTimestampObject = new java.sql.Timestamp(calendar.getTime().getTime());
		    pStmt.setTimestamp(1, ourJavaTimestampObject);  		 
			pStmt.executeUpdate();  
			conn.close();
		} catch (Exception e) {
			System.out.println("database error");
			e.printStackTrace();
		}
		 */
    }
    public void doPost(HttpServletRequest req, HttpServletResponse resp)
                throws ServletException,IOException
    {
        doGet(req,resp);
    }
	
    private int getCurrentCategory(HttpServletRequest req) 
				throws ServletException,IOException
	{
		HttpSession session = req.getSession(true);
		if (session.getAttribute("lastcat")!=null)
			return Integer.parseInt(session.getAttribute("lastcat").toString());
		else
			return -1;
			
	}	
}