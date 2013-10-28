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

public class count_category extends HttpServlet
{		   			
    public void doGet(HttpServletRequest req, HttpServletResponse resp)
               throws ServletException,IOException
    {	
		String idParam = "ids";
		String thParam = "ths";
			
		String ids = req.getParameter(idParam);
		String ths = req.getParameter(thParam);
		String queryStr = req.getQueryString();
		int a= queryStr.indexOf("solr=");
		String solrs = queryStr.substring(a+5);
		System.out.println(ids);
		System.out.println(ths);		
		System.out.println(solrs);				
			
		//count child facet	
		//id1[0.5 TO 1], id2[0.5 TO 1]
		String [] ids_str_list = ids.split("_");
		String [] ths_str_list = ths.split("_");
		int [] numFounds = new int [ids_str_list.length];
		for(int i =0;i<ids_str_list.length;i++)
		{
			//setup search request
			String query = solrs + "&fq=cat_prob_"+ids_str_list[i] + ":[" + ths_str_list[i] + "%20TO%201]&fl=id";
			System.out.println(query);
			HttpClient httpclient = new DefaultHttpClient();
			HttpGet httpget = new HttpGet(query);
			HttpResponse response = httpclient.execute(httpget);
			HttpEntity entity = response.getEntity();
			String content = "";
			if (entity != null) {
				InputStream instream = entity.getContent();
				BufferedReader br = new BufferedReader(new InputStreamReader(instream));
				String tmp_str = "";				
				while ((tmp_str = br.readLine()) != null) {				
					content += tmp_str;						
				}				
				//parse number found
				int c = content.indexOf("numFound");
				String str_int = content.substring(c+10);
				int b= str_int.indexOf(",");
				String final_int = str_int.substring(0, b);
				int count = Integer.parseInt(final_int);				
				numFounds[i] = count;
			}
		}
		//count other facet
		//id1[0 TO 0.5], id2[0 TO 0.5]
		//setup search request
		String query = solrs;
		int otherCount = 0;
		for(int i =0;i<ids_str_list.length;i++)
			query += "&fq=cat_prob_"+ids_str_list[i] + ":[0%20TO%20" + ths_str_list[i] + "]&fl=id";
		System.out.println(query);
		HttpClient httpclient = new DefaultHttpClient();
		HttpGet httpget = new HttpGet(query);
		HttpResponse response = httpclient.execute(httpget);
		HttpEntity entity = response.getEntity();
		String content = "";
		if (entity != null) {
			InputStream instream = entity.getContent();
			BufferedReader br = new BufferedReader(new InputStreamReader(instream));
			String tmp_str = "";				
			while ((tmp_str = br.readLine()) != null) {				
				content += tmp_str;						
			}			
			//parse number found
			int c = content.indexOf("numFound");
			String str_int = content.substring(c+10);
			int b= str_int.indexOf(",");
			String final_int = str_int.substring(0, b);
			otherCount = Integer.parseInt(final_int);
		}
		//make json ouput
		String output = "numfound={";
		for(int i=0;i<numFounds.length;i++)
		{
			output += "\"" + ids_str_list[i] + "\":" + numFounds[i] + ",";
		}
		output += "\"-1\":" + otherCount + "}";		
		resp.addHeader("Content-Type", "text/javascript");
		resp.addHeader("Access-Control-Allow-Origin", "*");			
				
		java.io.PrintWriter pw=resp.getWriter();
		pw.write(output);								
		pw.close();			
	}
}
