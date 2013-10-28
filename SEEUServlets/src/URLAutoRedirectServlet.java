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

public class URLAutoRedirectServlet extends HttpServlet
{
	static Connection conn = null;
	static String url = "jdbc:mysql://192.168.0.2:3306/";
	static String dbName = "seeu";
	static String driver = "com.mysql.jdbc.Driver";
	static String userName = "root"; 
	static String password = "see";
	static String tb = "comments";
	static String mobile_url = "http://kdd.csd.uwo.ca:88/seeumobile/index.html";
    static String pc_url = "http://kdd.csd.uwo.ca:88/seeu/index.html";
    static String mobile_user_agent_pattern = "";
    static String mobile_user_agent_pattern_first_four = "";
	 // This method is called by the servlet container just before this servlet
	 // is put into service.
	 public void init() throws ServletException {	
	     // The int parameters can also be retrieved using the servlet context
		 dbName = getServletConfig().getInitParameter("db_schema");
		 tb = getServletConfig().getInitParameter("db_tb");
		 mobile_url = getServletConfig().getInitParameter("mobile_url");
		 pc_url = getServletConfig().getInitParameter("pc_url");
		 mobile_user_agent_pattern = getServletConfig().getInitParameter("mobile_user_agent_pattern");
		 mobile_user_agent_pattern_first_four = getServletConfig().getInitParameter("mobile_user_agent_pattern_first_four");
		 System.out.println("Mobile User Agent Pattern is:\n" + mobile_user_agent_pattern + "\n" + mobile_user_agent_pattern_first_four);
	 }
	 
	 public boolean match_mobile_user_agent(String text)
	{
		//String p1 = "(?i).*(android.+mobile|avantgo|bada\\/|blackberry|blazer|compal|elaine|fennec|hiptop|iemobile|ip(hone|od)|iris|kindle|lge |maemo|meego.+mobile|midp|mmp|netfront|opera m(ob|in)i|palm( os)?|phone|p(ixi|re)\\/|plucker|pocket|psp|series(4|6)0|symbian|treo|up\\.(browser|link)|vodafone|wap|windows (ce|phone)|xda|xiino).*";
		//String p2 = "(?i)1207|6310|6590|3gso|4thp|50[1-6]i|770s|802s|a wa|abac|ac(er|oo|s\\-)|ai(ko|rn)|al(av|ca|co)|amoi|an(ex|ny|yw)|aptu|ar(ch|go)|as(te|us)|attw|au(di|\\-m|r |s )|avan|be(ck|ll|nq)|bi(lb|rd)|bl(ac|az)|br(e|v)w|bumb|bw\\-(n|u)|c55\\/|capi|ccwa|cdm\\-|cell|chtm|cldc|cmd\\-|co(mp|nd)|craw|da(it|ll|ng)|dbte|dc\\-s|devi|dica|dmob|do(c|p)o|ds(12|\\-d)|el(49|ai)|em(l2|ul)|er(ic|k0)|esl8|ez([4-7]0|os|wa|ze)|fetc|fly(\\-|_)|g1 u|g560|gene|gf\\-5|g\\-mo|go(\\.w|od)|gr(ad|un)|haie|hcit|hd\\-(m|p|t)|hei\\-|hi(pt|ta)|hp( i|ip)|hs\\-c|ht(c(\\-| |_|a|g|p|s|t)|tp)|hu(aw|tc)|i\\-(20|go|ma)|i230|iac( |\\-|\\/)|ibro|idea|ig01|ikom|im1k|inno|ipaq|iris|ja(t|v)a|jbro|jemu|jigs|kddi|keji|kgt( |\\/)|klon|kpt |kwc\\-|kyo(c|k)|le(no|xi)|lg( g|\\/(k|l|u)|50|54|\\-[a-w])|libw|lynx|m1\\-w|m3ga|m50\\/|ma(te|ui|xo)|mc(01|21|ca)|m\\-cr|me(di|rc|ri)|mi(o8|oa|ts)|mmef|mo(01|02|bi|de|do|t(\\-| |o|v)|zz)|mt(50|p1|v )|mwbp|mywa|n10[0-2]|n20[2-3]|n30(0|2)|n50(0|2|5)|n7(0(0|1)|10)|ne((c|m)\\-|on|tf|wf|wg|wt)|nok(6|i)|nzph|o2im|op(ti|wv)|oran|owg1|p800|pan(a|d|t)|pdxg|pg(13|\\-([1-8]|c))|phil|pire|pl(ay|uc)|pn\\-2|po(ck|rt|se)|prox|psio|pt\\-g|qa\\-a|qc(07|12|21|32|60|\\-[2-7]|i\\-)|qtek|r380|r600|raks|rim9|ro(ve|zo)|s55\\/|sa(ge|ma|mm|ms|ny|va)|sc(01|h\\-|oo|p\\-)|sdk\\/|se(c(\\-|0|1)|47|mc|nd|ri)|sgh\\-|shar|sie(\\-|m)|sk\\-0|sl(45|id)|sm(al|ar|b3|it|t5)|so(ft|ny)|sp(01|h\\-|v\\-|v )|sy(01|mb)|t2(18|50)|t6(00|10|18)|ta(gt|lk)|tcl\\-|tdg\\-|tel(i|m)|tim\\-|t\\-mo|to(pl|sh)|ts(70|m\\-|m3|m5)|tx\\-9|up(\\.b|g1|si)|utst|v400|v750|veri|vi(rg|te)|vk(40|5[0-3]|\\-v)|vm40|voda|vulc|vx(52|53|60|61|70|80|81|83|85|98)|w3c(\\-| )|webc|whit|wi(g |nc|nw)|wmlb|wonu|x700|yas\\-|your|zeto|zte\\-"; 
		//if(text.matches(p1)||text.substring(0,4).matches(p2))
		if(text.matches(mobile_user_agent_pattern)||text.substring(0,4).matches(mobile_user_agent_pattern_first_four))
		{
			return true;		
		}
		else
		{
			return false;						
		}
	}
	 
	public void doGet(HttpServletRequest req, HttpServletResponse resp)
               throws ServletException,IOException
    {								
		String desc="SEEU Version 0.1 Portal Redirect";
		String ip = req.getRemoteAddr();
		
		//get session ID
		HttpSession session = req.getSession(true);
		String sessionid = session.getId();	
		
		//get http header
		ArrayList<String> head_lst = new ArrayList<String>();						
		Enumeration<String> enu = (Enumeration<String>)req.getHeaderNames();
		boolean first = true;
		String ua = "";
		while(enu.hasMoreElements())
		{
			String head_name = (String)enu.nextElement();
			String head_value = req.getHeader(head_name);
			head_lst.add("<" + head_name + ">" + head_value + "</>");
			if(head_name.equals("User-Agent") == true)
			{
				ua = head_value.toLowerCase();
			}
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
		
		System.out.println("receive user loging portal" + ip);
		
		//analyze the user agent
		//set up the correct URL position
		String url_to_dest = "";
		if(match_mobile_user_agent(ua))
		{
			url_to_dest = mobile_url;			
		}
		else
		{
			url_to_dest = pc_url;						
		}
		
		//write statistic into database
		try{
			Class.forName(driver).newInstance();
			conn = DriverManager.getConnection(url+dbName,userName,password);				
				
			//insert into tb
			PreparedStatement pStmt = conn.prepareStatement("insert into "+tb+"(sessionid, time, ip, desturl, httpheaders, description) values(?,?,?,?,?,?)");
			pStmt.setString(1, sessionid);
			java.sql.Timestamp timestamp = new java.sql.Timestamp(Calendar.getInstance().getTime().getTime());
		    pStmt.setTimestamp(2, timestamp);
			pStmt.setString(3, ip);
			pStmt.setString(4, url_to_dest);
			pStmt.setString(5, headers);
			pStmt.setString(6, desc);						    		   
		    
		    System.out.println(pStmt.toString());
		    
		    pStmt.executeUpdate();
																																	
			conn.close();
		} catch (Exception e) {
			System.out.println("database error");
			e.printStackTrace();
		}
		
		//jump to the new URL		
		resp.sendRedirect(url_to_dest);
		
		 return;
    }
    public void doPost(HttpServletRequest req, HttpServletResponse resp)
                throws ServletException,IOException
    {
        doGet(req,resp);
    }	   
}