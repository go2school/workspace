import javax.servlet.http.*;
import javax.servlet.*;
import java.io.*;
import java.net.*;
import java.sql.*;
import java.util.ArrayList;
import java.lang.Double;
import java.util.Calendar;

public class RegisterServlet extends HttpServlet
{
	static Connection conn = null;
	static String url = "jdbc:mysql://192.168.0.2:3306/";
	static String dbName = "seeu";
	static String driver = "com.mysql.jdbc.Driver";
	static String userName = "root"; 
	static String password = "see";
	static String usertb = "users";
    static String dest = "http://kdd.csd.uwo.ca:88/seeu/index.html";
    
	public void doGet(HttpServletRequest req, HttpServletResponse resp)
               throws ServletException,IOException
    {		
		String name=req.getParameter("username");
		String pass=req.getParameter("password");
		String prof=req.getParameter("prof");
		String email=req.getParameter("email");
		
		System.out.println("user register" + name + " " + pass + " " + prof + " " + email);
		
		String msg = "";
		if(name == null || name.equals(""))
		{				
			msg = "Error! User name is empty.";							
		}
		else if(pass == null || pass.equals(""))
		{				
			msg = "Error! Password is empty.";				
		}	
		else
		{		
			//check if user exist
			try{
				Class.forName(driver).newInstance();
				conn = DriverManager.getConnection(url+dbName,userName,password);				
				
				boolean is_exist = true;
				Statement st = conn.createStatement();
				String sql = "select * from "+usertb+" where name=\"" + name + "\"";
				ResultSet rs = st.executeQuery(sql);
				if(rs.next() == true)
				{
					msg = "Error! The user is already registered!";
					is_exist = true;
				}	
				else
					is_exist = false;
				rs.close();
				st.close();
				
				if(is_exist == false)
				{
					if(prof == null)
						prof = "";
					if(email == null)
						email = "";
					
					//insert into tb
					PreparedStatement pStmt = conn.prepareStatement("insert into "+usertb+"(name,password,prof,email) values(?,?,?,?)");
					pStmt.setString(1, name);
					pStmt.setString(2, pass);
					pStmt.setString(3, prof);
					pStmt.setString(4, email);
					pStmt.executeUpdate();
										
					msg = "<html>"
							+ "<head>"
							+ "<title>A web page that points a browser to a different page after 2 seconds</title>"
							+ "<meta http-equiv=\"refresh\" content=\"2; URL=" + dest + "\">"
							+ "<meta name=\"keywords\" content=\"automatic redirection\">"
							+ "</head>"
							+ "<body>"
							+ "If your browser doesn't automatically go there within a few seconds," 
							+ "you may want to go to "
							+ "<a href=\"" + dest + "\">SEEU</a>" 
							+ "manually."
							+ "</body>"
							+ "</html>";										
				}								
				conn.close();
			} catch (Exception e) {
				System.out.println("database error");
				e.printStackTrace();
			}
		}		
		
		java.io.PrintWriter pw=resp.getWriter();				
		pw.write(msg);
		pw.close();				
		
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
}