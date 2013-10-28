import javax.servlet.*;
import javax.servlet.http.*;
import java.io.*;
import java.net.*;
import java.util.*;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;

public class count_category_list extends HttpServlet
{		   			
    public void doGet(HttpServletRequest req, HttpServletResponse resp)
               throws ServletException,IOException
    {			
    	//get nodes and thresholds
		String idParam = "ids";
		String thParam = "ths";
			
		//get node parameters
		String ids = req.getParameter(idParam);
		String ths = req.getParameter(thParam);
		
		//get solr URL
		String queryStr = req.getQueryString();
		int a= queryStr.indexOf("solr=");
		String solrs = queryStr.substring(a+5);
		
		System.out.println(ids);
		System.out.println(ths);						
		System.out.println(solrs);				
		
		//split parameter
		String [] node_list = null;
		String [] th_list = null;
		int [] nd_id_list = null;
		int [] nd_count_list = null;
		
		node_list = ids.split("_");
		th_list = ths.split("_");
		
		nd_id_list = new int [node_list.length];
		nd_count_list = new int [node_list.length];
		
		//for each node, query solr
		for(int i=0;i<node_list.length;i++)
		{
			String [] cat_list = node_list[i].split(":");
			String current_node = cat_list[cat_list.length - 1];
			int int_current_node = Integer.parseInt(current_node);
			//make solr query parameter
			String query_url = solrs;
			for(int j=0;j<cat_list.length;j++)
				query_url = query_url + "&fq=cat_prob_"+cat_list[j] + ":[" + th_list[i] + "%20TO%201]";
			System.out.println(query_url);
			HttpClient httpclient = new DefaultHttpClient();
			HttpGet httpget = new HttpGet(query_url + "&fl=id");
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
				nd_id_list[i] = int_current_node;
				nd_count_list[i] = count;
			}
			else
			{
				nd_id_list[i] = -1;
				nd_count_list[i] = 0;
			}						
		}
		
		//make json ouput
		String output = "numfound={";
		//get parent cat count		
		if(nd_count_list != null)
		{
			//get child cat count
			for(int i=0;i<nd_count_list.length;i++)
			{
				output += "\"" + nd_id_list[i] + "\":" + nd_count_list[i] + ",";
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
