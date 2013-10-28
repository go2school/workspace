import javax.servlet.*;
import javax.servlet.http.*;
import java.io.*;
import java.net.*;
import java.util.*;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.conn.params.ConnRoutePNames;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;

public class callgooglewestern_action extends HttpServlet
{		   			
	
	static String actionservlet_url = "http://kdd.csd.uwo.ca:88/seevsgoogle/servlet/googleaction/";
	
    public void doGet(HttpServletRequest req, HttpServletResponse resp)
               throws ServletException,IOException
    {	
		String queryStr = req.getQueryString();
		String search_string = "";
		if(queryStr.startsWith("search?q=") == false)
			search_string = "http://find.uwo.ca/search?q="+queryStr+"&btnG=Google+Search+Western&client=default_frontend&output=xml_no_dtd&proxystylesheet=default_frontend&sort=date%3AD%3AL%3Ad1&entqr=0&entqrm=0&oe=UTF-8&ie=UTF-8&ud=1&site=default_collection";			
		else
			search_string = "http://find.uwo.ca/"+queryStr;
		
		System.out.println(search_string);		
						
		HttpClient httpclient = new DefaultHttpClient();
		HttpGet httpget = new HttpGet(search_string);
		HttpResponse response = httpclient.execute(httpget);
		HttpEntity entity = response.getEntity();
		String content = "";
		if (entity != null) {
			InputStream instream = entity.getContent();
			BufferedReader br = new BufferedReader(new InputStreamReader(instream));
			String tmp_str = "";
			boolean skipnext = false;
			while ((tmp_str = br.readLine()) != null) {						
				//remove image, news, local
				if(skipnext == true)
				{
					skipnext = false;
					continue;
				}
				//add my customzed code
				if(tmp_str.contains("<html><head>") == true)
				{
					tmp_str += "<script type=\"text/javascript\" src=\"http://ajax.googleapis.com/ajax/libs/jquery/1.3.2/jquery.min.js\"></script>\n";					
					tmp_str += "<script type=\"text/javascript\" src=\"callgoogle.js\"></script>";
				}					
				else if(tmp_str.contains("<td><font size=\"-1\"><a class=\"q\"") == true)
				{
					tmp_str = "<td><p></p></td>\n";
					skipnext = true;
				}
				//remove form
				else if(tmp_str.contains("<form name=\"gs\" method=\"GET\" action=\"search\">") == true)
				{
					tmp_str = "";					
				}				
				//remove form
				else if(tmp_str.contains("<input type=\"hidden\" name=\"site\"") == true)			
				{
					tmp_str = "<input type=\"hidden\" name=\"site\" value=\"default_collection\">";
				}
				//remove advanced search
				else if(tmp_str.contains("<td nowrap=\"1\"><font size=\"-2\">&nbsp;&nbsp") == true)
				{
					tmp_str = "";					
				}
				//change search button
				else if(tmp_str.contains("<td valign=\"middle\"><font size=\"-1\">&nbsp;<input type=\"submit\"") == true)			
				{
					tmp_str = "<td valign=\"middle\"><font size=\"-1\"><input type= \"button\" name=\"btnG\" value=\"Google Search Western\" onclick=\"search_google()\"/></font></td>";
				}	
				//change navigation
				else if(tmp_str.contains("<td><a ctype=\"nav.page\" href") == true)			
				{
					int b = tmp_str.indexOf("href=");
					tmp_str = "<td><a ctype=\"nav.page\" href=\"#\" name=" + tmp_str.substring(b+5);
				}				
				else if(tmp_str.contains("<td nowrap=\"1\"><span class=\"b\"><a ctype=\"nav.prev\" href") == true)			
				{
					int b = tmp_str.indexOf("href=");
					tmp_str = "<td nowrap=\"1\"><span class=\"b\"><a ctype=\"nav.prev\" href=\"#\" name=" + tmp_str.substring(b+5);
				}
				else if(tmp_str.contains("<td nowrap=\"1\"><span class=\"b\"><a ctype=\"nav.next\" href") == true)			
				{
					int b = tmp_str.indexOf("href=");
					tmp_str = "<td nowrap=\"1\"><span class=\"b\"><a ctype=\"nav.next\" href=\"#\" name=" + tmp_str.substring(b+5);
				}
				//change top navigation
				else if(tmp_str.contains("<td nowrap=\"1\"><span class=\"s\"><a ctype=\"nav.prev\" href") == true)			
				{
					int b = tmp_str.indexOf("href=");
					tmp_str = "<td nowrap=\"1\">&nbsp;<span class=\"s\"><a ctype=\"nav.prev\" href=\"#\" name=" + tmp_str.substring(b+5);
				}
				else if(tmp_str.contains("<td nowrap=\"1\">&nbsp;<span class=\"s\"><a ctype=\"nav.next\" href") == true)			
				{
					int b = tmp_str.indexOf("href=");
					tmp_str = "<td nowrap=\"1\"><span class=\"s\"><a ctype=\"nav.next\" href=\"#\" name=" + tmp_str.substring(b+5);
				}
				//change sort by date				
				else if(tmp_str.contains("<td><span class=\"s\"><a ctype=\"sort\" href") == true)			
				{
					int b = tmp_str.indexOf("href=");
					tmp_str = "<td><span class=\"s\"><a ctype=\"sort\" href=\"#\" name=" + tmp_str.substring(b+5);
				}
				//change sort by relevance				
				else if(tmp_str.contains("<td><span class=\"s\"><font color=\"#000000\">Sort by date / </font><a ctype=\"sort\" href") == true)			
				{
					int b = tmp_str.indexOf("href=");
					tmp_str = "<td><span class=\"s\"><font color=\"#000000\">Sort by date / </font><a ctype=\"sort\" href=\"#\" name=" + tmp_str.substring(b+5);
				}
				//add script before html
				else if(tmp_str.startsWith("</html>") == true)
				{
					tmp_str = "<script type=\"text/javascript\">$('input[name=\"q\"]').bind('keydown', function(e){var code = (e.keyCode ? e.keyCode : e.which);if(code == 13) {search_google();}});</script>\n" + tmp_str;					
					tmp_str = "<script type=\"text/javascript\">var vs=$('a[ctype=\"nav.page\"]');if(vs!=undefined)for(var i=0;i<vs.length;i++){vs[i].onclick = function(){search_google_2($(this)[0].name);};}</script>\n" + tmp_str;
					tmp_str = "<script type=\"text/javascript\">var vs=$('a[ctype=\"nav.prev\"]');if(vs!=undefined)for(var i=0;i<vs.length;i++){vs[i].onclick = function(){search_google_2($(this)[0].name);};}</script>\n" + tmp_str;
					tmp_str = "<script type=\"text/javascript\">var vs=$('a[ctype=\"nav.next\"]');if(vs!=undefined)for(var i=0;i<vs.length;i++){vs[i].onclick = function(){search_google_2($(this)[0].name);};}</script>\n" + tmp_str;
					tmp_str = "<script type=\"text/javascript\">var vs=$('a[ctype=\"sort\"]');if(vs!=undefined)for(var i=0;i<vs.length;i++){vs[i].onclick = function(){search_google_2($(this)[0].name);};}</script>\n" + tmp_str;
					
					tmp_str = "<script type=\"text/javascript\">var query_text = $('input[name=\"q\"]').val();\n"
							+ "var num_page = $('.i').text();\n"
							+ "$('a[ctype=\"c\"]').click(function(event){\n$.post(\""+actionservlet_url+"\", { user: \"\", query:query_text, link:$(this).attr('href'),  page:num_page, rank:$(this).attr('rank'), description: \"google click\"});return true;})\n"
							+ "$('a[ctype=\"keymatch\"]').click(function(event){\n$.post(\""+actionservlet_url+"\", { user: \"\", query:query_text, link:$(this).attr('href'),  page:num_page, rank:0, description: \"google click\"});\n"
							+ "return true;\n});</script>\n" + tmp_str;					
				}
				
				/*
				else if(tmp_str.contains("<table border=\"0\" cellpadding=\"0\" cellspacing=\"0\"") == true)
				{
					tmp_str = "<table id=\"hiddentable\" border=\"0\" cellpadding=\"0\" cellspacing=\"0\" style=\"display:none\"";
				}
				*/
				
				content += tmp_str;						
			}
			resp.addHeader("Content-Type", "text/javascript");
			resp.addHeader("Access-Control-Allow-Origin", "*");			
			
			//replace all pics			
			content = content.replaceAll("/nav_first.gif", "/seevsgoogle/nav_first.gif");
			content = content.replaceAll("/nav_current.gif", "/seevsgoogle/nav_current.gif");
			content = content.replaceAll("/nav_page.gif", "/seevsgoogle/nav_page.gif");
			content = content.replaceAll("/nav_next.gif", "/seevsgoogle/nav_next.gif");
			content = content.replaceAll("/nav_previous.gif", "/seevsgoogle/nav_previous.gif");
			java.io.PrintWriter pw=resp.getWriter();
			pw.write(content);								
			pw.close();
			
			PrintWriter pw2 = new PrintWriter(new FileWriter("a.tmp"));
			pw2.write(content);
			pw2.close();
		}	
	}
}
