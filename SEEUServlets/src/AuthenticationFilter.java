// Import required java libraries
import java.io.*;
import javax.servlet.*;
import javax.servlet.http.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.*;
import java.sql.*;

// Implements Filter class
public class AuthenticationFilter implements Filter  {
   

	public String MSG_WRONG_USERID = "wuser";
	public String MSG_WRONG_PASS = "wpass";
	public String MSG_CORRECT = "yes";
	public TreeSet<String> exception_lst = new TreeSet<String>();
	public String login_page = "";
	
	public void closeConnection(PreparedStatement psdoLogin, ResultSet rsdoLogin, Connection conn)
	{
		try{
			 if(psdoLogin!=null){
				 psdoLogin.close();
			 }
			 if(rsdoLogin!=null){
				 rsdoLogin.close();
			 }
			 
			 if(conn!=null){
			  conn.close();
			 }
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}	
	}	
	
	public boolean isLogin(HttpSession session)
	{
		String userID = (String)session.getAttribute("UserID");
		//get session
		String UserName = (String)session.getAttribute("UserName");
		String UserType = (String)session.getAttribute("UserType");
		String UserLevel = (String)session.getAttribute("UserLevel");			 
		if(userID != null)
			return true;
		else
			return false;
	}

	public void init(FilterConfig config) 
                         throws ServletException{
      //read exception URL list    
      String excep_url_lst = config.getInitParameter("exception_url_lst");
      String [] urls = excep_url_lst.split(";");
      for(int i=0;i<urls.length;i++)
      {
    	  exception_lst.add(urls[i]);
    	  System.out.println("Exception url " + urls[i]);
      }
      
      //read forwarding url 
      String forward_url = config.getInitParameter("login_page");
      login_page = forward_url;
   }
	
   public void  doFilter(ServletRequest request, 
                 ServletResponse response,
                 FilterChain chain) 
                 throws java.io.IOException, ServletException {
 
	   HttpServletRequest hsr = (HttpServletRequest)request;
	   HttpSession session = hsr.getSession();
	   String url = hsr.getRequestURL().toString();
	   
	   //check if this url exist in exception list;
	   Iterator<String> it = exception_lst.iterator();
	   boolean found = false;
	   while(it.hasNext())
	   {
		   String k = it.next();
		   if(url.endsWith(k))
		   {
			   found = true;
			   break;
		   }
	   }
	   
	   boolean shouldDoChain = true;
	   if(found == false)
	   {
		   if(isLogin(session) == false)
	       {
	    		//if the user is not logined, forward to login UI		
			   HttpServletResponse hres = (HttpServletResponse)response;
			   System.out.println("Redirect to login.html");
			   shouldDoChain = false;
	    	   hres.sendRedirect(login_page);		   
	       }
		   else
			   System.out.println("Open page " + url);
	   }
	   else
		   System.out.println("Open page " + url);
	   
      //Pass request back down the filter chain
	   if(shouldDoChain == true)
		   chain.doFilter(request,response);
   }
   public void destroy( ){
      /* Called before the Filter instance is removed 
      from service by the web container*/
   }  
}