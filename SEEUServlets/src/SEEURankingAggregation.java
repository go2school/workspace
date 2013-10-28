import javax.servlet.*;
import javax.servlet.http.*;
import java.io.*;
import java.net.*;
import java.util.*;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;

public class SEEURankingAggregation extends HttpServlet
{	
	
	public String getContent(String query) throws ClientProtocolException, IOException
	{
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
		}
		return content;
	}
	
    public void doGet(HttpServletRequest req, HttpServletResponse resp)
               throws ServletException,IOException
    {			
    	//aggregate results from difference universities
    	
    	//get nodes and thresholds
		String unameParam = "uname";
		String ucountParam = "ucount";
			
		//get university parameters
		String uname_para = req.getParameter(unameParam);
		String ucount_para = req.getParameter(ucountParam);
		System.out.println(uname_para);
		System.out.println(ucount_para);
		
		//get solr URL
		String queryStr = req.getQueryString();
		int a= queryStr.indexOf("solr=");
		String solrs = queryStr.substring(a+5);							
		System.out.println(solrs);				
		
		//split parameter
		String [] uname_list = null;
		String [] ucount_list = null;		
		int [] nd_ucount_list_list = null;
		
		uname_list = uname_para.split("_");
		ucount_list = ucount_para.split("_");
		
		nd_ucount_list_list = new int [ucount_list.length];
		for(int i=0;i<uname_list.length;i++)
			nd_ucount_list_list[i] = Integer.parseInt(ucount_list[i]);
		
		//for each university, query solr
		for(int i=0;i<uname_list.length;i++)
		{
			String uname = uname_list[i];
			
			//make solr query parameter
			String query_url = solrs + "&fq=uname:" + uname + "&rows=" + nd_ucount_list_list[i] ;
						
			System.out.println(query_url);
			
			String content = getContent(query_url + "&fl=id,score");
			if(content.equals(""))
			{
				
			}
			else
			{
				
			}								
		}
		
		//make json ouput
		String output = "numfound={";
		
		
		output += "}";
		resp.addHeader("Content-Type", "text/javascript");
		resp.addHeader("Access-Control-Allow-Origin", "*");			
				
		java.io.PrintWriter pw=resp.getWriter();
		pw.write(output);								
		pw.close();			
	}    
}
