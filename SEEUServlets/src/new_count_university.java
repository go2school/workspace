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

public class new_count_university extends HttpServlet
{		   			
    public void doGet(HttpServletRequest req, HttpServletResponse resp)
               throws ServletException,IOException
    {	
		String pareIDParam = "parids";
		String pareThParam = "parths";
		String idParam = "ids";
		String thParam = "ths";
			
		String ids = req.getParameter(idParam);
		String ths = req.getParameter(thParam);
		String parIds = req.getParameter(pareIDParam);
		String parThs = req.getParameter(pareThParam);
		String queryStr = req.getQueryString();
		
		int a= queryStr.indexOf("solr=");
		String solrs = queryStr.substring(a+5);
				
		System.out.println(ids);
		System.out.println(ths);		
		System.out.println(parIds);
		System.out.println(parThs);
		System.out.println(solrs);				
			
		//count parent facet
		//id1[0.5 TO 1], id2[0.5 TO 1]
		String [] par_ids_str_list = null;
		String [] par_ths_str_list = null;
		int [] par_numFounds = null;
		
		String incSolrs = solrs;		
		//count ancestor cats
		if(parIds != null && parThs != null)
		{
			par_ids_str_list = parIds.split("_");
			par_ths_str_list = parThs.split("_");
			par_numFounds = new int [par_ids_str_list.length];		
				
			for(int i =0;i<par_ids_str_list.length;i++)
			{
				//setup search request
				incSolrs = incSolrs + "&fq=org_"+par_ids_str_list[i] + ":" + par_ths_str_list[i];
				
				HttpClient httpclient = new DefaultHttpClient();
				HttpGet httpget = new HttpGet(incSolrs + "&fl=id");
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
					par_numFounds[i] = count;
				}
			}
		}	
			
		String [] ids_str_list = null;
		String [] ths_str_list = null;
		int [] numFounds = null;
		if(ids != null && ths != null)
		{
			ids_str_list = ids.split("_");
			ths_str_list = ths.split("_");
			numFounds = new int [ids_str_list.length];
			for(int i =0;i<ids_str_list.length;i++)
			{
				//setup search request
				String query = incSolrs + "&fq=org_"+ids_str_list[i] + ":" + ths_str_list[i] + "&fl=id";
				
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
					System.out.println(content);
					//parse number found
					int c = content.indexOf("numFound");
					String str_int = content.substring(c+10);
					int b= str_int.indexOf(",");
					String final_int = str_int.substring(0, b);
					int count = Integer.parseInt(final_int);				
					numFounds[i] = count;
				}
			}
		}
		
		//make json ouput
		String output = "numfound = {";
		
		if(par_numFounds != null)
		{		
			for(int i=0;i<par_numFounds.length;i++)
			{
				output += "\"" + par_ids_str_list[i] + "\":" + par_numFounds[i] + ",";
			}		
		}
		if(numFounds != null)
		{
			//get child cat count
			for(int i=0;i<numFounds.length;i++)
			{
				output += "\"" + ids_str_list[i] + "\":" + numFounds[i] + ",";
			}			
		}
		
		output += "}";
		resp.addHeader("Content-Type", "text/javascript");
		resp.addHeader("Access-Control-Allow-Origin", "*");			
				
		java.io.PrintWriter pw=resp.getWriter();
		pw.write(output);								
		pw.close();			
	}
}
