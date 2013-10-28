import javax.servlet.http.*;
import javax.servlet.*;
import java.io.*;
import java.net.*;
import java.sql.*;
import java.util.ArrayList;
import java.lang.Double;
import java.util.Calendar;

public class TestLoginServlet extends HttpServlet
{	
	public void doGet(HttpServletRequest req, HttpServletResponse resp)
               throws ServletException,IOException
    {		
		HttpSession session = req.getSession(false);
		if (session != null && session.getAttribute("name")!=null)
		{
			String name = (String)session.getAttribute("name");
			java.io.PrintWriter pw=resp.getWriter();
			pw.write(name);								
			pw.close();	
		}
		else
		{
			java.io.PrintWriter pw=resp.getWriter();
			pw.write("");								
			pw.close();
		}
    }   
}