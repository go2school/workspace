import javax.servlet.http.*;
import javax.servlet.*;
import java.io.*;
import java.net.*;
import java.sql.*;
import java.util.ArrayList;
import java.lang.Double;
import java.util.Calendar;

public class LoginServlet extends HttpServlet
{
	static Connection conn = null;
	static String url = "jdbc:mysql://192.168.0.2:3306/";
	static String dbName = "seeu";
	static String driver = "com.mysql.jdbc.Driver";
	static String userName = "root";
	static String usertb = "users";
	static String password = "see";
	static String dest = "http://kdd.csd.uwo.ca:88/seeu/index.html";
    
	public void doGet(HttpServletRequest req, HttpServletResponse resp)
               throws ServletException,IOException
    {		
		String name=req.getParameter("username");
		String pass=req.getParameter("password");
		
		String msg = "";
		if(name == null)
		{				
			msg = "Error! User name is empty.";							
		}
		else if(pass == null)
		{				
			msg = "Error! Password is empty.";				
		}
		else
		{		
			//check if user exist
			try{
				Class.forName(driver).newInstance();
				conn = DriverManager.getConnection(url+dbName,userName,password);				
				
				boolean is_exist = false;
				Statement st = conn.createStatement();
				String sql = "select * from "+usertb+" where name=\"" + name + "\" and password = \"" + pass + "\"";
				ResultSet rs = st.executeQuery(sql);
				if(rs.next() == false)
				{
					msg = "Error! The username or the password is not correct";
					
					is_exist = false;
				}	
				else
					is_exist = true;
				rs.close();
				st.close();
				
				if(is_exist == true)
				{					
					//invalidate the old session first					
					HttpSession oldsession = req.getSession(false);
					if(oldsession != null)
						oldsession.invalidate();
					//create a new session and insert the username into this session
					HttpSession session = req.getSession(true);
					session.setAttribute("name", name);
					
					msg = "<html>"
							+ "<head>"
							+ "<title>A web page that points a browser to a different page after 2 seconds</title>"
							+ "<meta http-equiv=\"refresh\" content=\"2; URL="+dest+"\">"
							+ "<meta name=\"keywords\" content=\"automatic redirection\">"
							+ "</head>"
							+ "<body>"
							+ "If your browser doesn't automatically go there within a few seconds," 
							+ "you may want to go to "
							+ "<a href=\""+dest+"\">SEEU</a>" 
							+ "manually."
							+ "</body>"
							+ "</html>";
					
					System.out.println("user login" + name + " " + pass);
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