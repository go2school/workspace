import javax.servlet.http.*;
import javax.servlet.*;

import java.io.*;
import java.net.*;
import java.sql.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.Hashtable;
import java.lang.Double;
import java.util.Calendar;

public class LinkClickActionLogServlet extends HttpServlet
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
		String qurl = req.getQueryString();
		System.out.println(qurl);
		
		int pos = qurl.indexOf("?");
		String para_str = qurl.substring(pos+1);
		String [] paras = para_str.split("&");
		Hashtable<String, String> parameters = new Hashtable<String, String>();		
		for(int i=0;i<paras.length;i++)
		{
			int p_pos = paras[i].indexOf("=");
			String p_name = paras[i].substring(0, p_pos);
			String p_value = URLDecoder.decode(paras[i].substring(p_pos+1));
			parameters.put(p_name, p_value);
		}
		String user=parameters.get("user");
		String query=parameters.get("query");
		String link=parameters.get("link");
		String curcat=parameters.get("curcat");
		String pageid=parameters.get("pageid");
		String page=parameters.get("page");
		String rank=parameters.get("rank");
		String solrparameter=parameters.get("solrparameter");
		String desc=parameters.get("description");
		
		System.out.println(user + '\n' + query + '\n' + link + '\n' + curcat + '\n' + pageid + '\n' + page + '\n' + rank + '\n' + solrparameter + '\n' + desc);
		
		String ip = req.getRemoteAddr();			
		
		//get HTTP header
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
		System.out.println(headers);
		
		if (user == null)
			user = "";
		if(ip == null)
			ip = "";			
		
		String msg = "Error";
		if(query == null || link == null || curcat == null || desc == null || pageid == null || solrparameter == null)
		{				
			msg = "Error";
			java.io.PrintWriter pw=resp.getWriter();				
			pw.write(msg);
			pw.close();
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
				PreparedStatement pStmt = conn.prepareStatement("insert into "+usertb+"(user, sessionid, time, curcat, pageid, link, page, rank, query, solrparameter, ip, httpheaders, description) values(?,?,?,?,?,?,?,?,?,?,?,?,?)");
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
			    pStmt.setString(12, headers);
			    pStmt.setString(13, desc);

			    System.out.println(pStmt.toString());
			    			    
			    pStmt.executeUpdate();
																	
				conn.close();
				
				msg = "Done";
				
				resp.sendRedirect(link);
				
			} catch (Exception e) {
				System.out.println("database error");
				e.printStackTrace();
			}
		}								
    }

	public static void main(String argv[])
	{
		String url = "http://kdd.csd.uwo.ca:88/?user=&query=*%3A*&link=http%3A//www.uwindsor.ca/&curcat=-1&pageid=uwinsor_29262&page=0&rank=0&description=click%20%5BResult%20Link%5D&solrparameter=cur_cat%3D-1%26q%3D*%253A*%26hl%3Dtrue%26hl.fl%3Dcontent%252Ctitle%26hl.fragsize%3D200%26facet%3Dtrue%26facet.field%3DcontentType%26facet.field%3Duname%26facet.limit%3D100%26facet.mincount%3D1%26json.nl%3Dmap%26sort%3Dproduct%28sum%28boost%252C0.01%29%252Cfactor_1%29%2520desc";
		int pos = url.indexOf("?");
		String para_str = url.substring(pos+1);
		String [] paras = para_str.split("&");
		Hashtable<String, String> parameters = new Hashtable<String, String>();		
		for(int i=0;i<paras.length;i++)
		{
			int p_pos = paras[i].indexOf("=");
			String p_name = paras[i].substring(0, p_pos);
			String p_value = URLDecoder.decode(paras[i].substring(p_pos+1));
			parameters.put(p_name, p_value);
		}
		String user=parameters.get("user");
		String query=parameters.get("query");
		String link=parameters.get("link");
		String curcat=parameters.get("curcat");
		String pageid=parameters.get("pageid");
		String page=parameters.get("page");
		String rank=parameters.get("rank");
		String solrparameter=parameters.get("solrparameter");
		String desc=parameters.get("description");
		
		System.out.println(user + '\n' + query + '\n' + link + '\n' + curcat + '\n' + pageid + '\n' + page + '\n' + rank + '\n' + solrparameter + '\n' + desc);
	}
}