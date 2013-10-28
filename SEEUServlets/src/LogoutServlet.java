import javax.servlet.http.*;
import javax.servlet.*;
import java.io.*;
import java.net.*;
import java.sql.*;
import java.util.ArrayList;
import java.lang.Double;
import java.util.Calendar;

public class LogoutServlet extends HttpServlet
{	
	static String dest = "http://kdd.csd.uwo.ca:88/seeu/index.html";
	
	public void doGet(HttpServletRequest req, HttpServletResponse resp)
               throws ServletException,IOException
    {		
		HttpSession session = req.getSession(false);
		if(session != null)
		{
			session.removeAttribute("name");
			session.invalidate();
		}
		
		String msg = "<html>"
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
		
		java.io.PrintWriter pw=resp.getWriter();				
		pw.write(msg);
		pw.close();	
    }   
}