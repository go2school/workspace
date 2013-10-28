import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringWriter;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Hashtable;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;


public class TestRankAggregation {

	public class HTTPGetContentThread extends Thread {
		public String query = "";
		public String uname = "";
		public Hashtable<String, String> jsonObject = null;
	    public HTTPGetContentThread(String str, String uname, String query, Hashtable<String, String> json) {
			super(str);
			this.jsonObject = json;
			this.query = query;
			this.uname = uname;
			
	    }
	    public void run() 
	    {
			//call solr 
			String ret = null;
			try {
				ret = TestRankAggregation.getContent(query);
			} catch (ClientProtocolException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			jsonObject.put(uname, ret);
	    }
	}

	class MyObject implements Comparable 
	{
		public MyObject() {} 	 
		public String key = "";
		public double value = 0;
		
		public int compareTo(Object arg0) {
			// TODO Auto-generated method stub
			MyObject m = (MyObject)arg0;
			double diff = m.value - this.value;
			if(diff < 0)
				return -1;
			else if(diff > 0)
				return 1;
			else
				return 0;		
		}
	}

	public ArrayList<MyObject> string_to_list_object(String pin)
	{
		
		ArrayList<MyObject> vlst = new ArrayList<MyObject>();	
		if(pin.equals(""))
			return vlst;
		
		String [] fields = pin.split("},");
		for(int i=0;i<fields.length;i++)
		{
			//get id and score
			int a = fields[i].indexOf(":");
			int b = fields[i].indexOf(",");
			
			String name = fields[i].substring(a+2,b-1);
			
			//get score
			int last_score_pos = fields[i].lastIndexOf(":");
			String value = "";			
			if(i != fields.length-1)
			{				
				value = fields[i].substring(last_score_pos+1, fields[i].length());
			}
			else
				value = fields[i].substring(last_score_pos+1, fields[i].length()-1);
			
			MyObject m = new MyObject();
			m.key = name;;
			m.value = Double.parseDouble(value); 
			vlst.add(m);		
		}		
		
		return vlst;	
	}

	//HTTP Request helper
	public static String getContent(String query) throws ClientProtocolException, IOException
	{				
		HttpClient httpclient = new DefaultHttpClient();
		HttpGet httpget = new HttpGet(query);
		HttpResponse response = httpclient.execute(httpget);
		HttpEntity entity = response.getEntity();
		String content = "";
		if (entity != null) {
			BufferedReader br = new BufferedReader(new InputStreamReader(entity.getContent()));
			String tmp_str = "";				
			while ((tmp_str = br.readLine()) != null) {				
				content += tmp_str;						
			}
		}
		
		System.out.println(query);
		//String content = "{\"response\":{\"numFound\":278421,\"start\":0,\"maxScore\":1.0,\"docs\":[{\"id\":\"uwestern_104360\",\"score\":1.0}]}}";
		return content;
	}

	//simple merge and sort all document list together
	//return the topk documents
	ArrayList<MyObject> simple_merge(ArrayList<ArrayList<MyObject>> all_university_docs, int topk)
	{
		ArrayList<MyObject> lst = new ArrayList<MyObject>();
		//merge all docs together and sort it	
		for(int i=0;i<all_university_docs.size();i++)
		{
			for(int j=0;j<all_university_docs.get(i).size();j++)
			{
				lst.add(all_university_docs.get(i).get(j));			
			}		
		}
		//sort argain
		Collections.sort(lst);
			
		int sz = lst.size();
		if(sz > topk)
			sz = topk;
		ArrayList<MyObject> ret = new ArrayList<MyObject>();
		for(int i=0;i<sz;i++)
			ret.add(lst.get(i));
		return ret;
	}


	//make ABCABCABCABCABC list first
	//then sort the topk documents
	ArrayList<MyObject> roundrobin_merge(ArrayList<ArrayList<MyObject>> all_university_docs, int topk)
	{
		ArrayList<MyObject> lst = new ArrayList<MyObject>();
		//merge all docs together and sort it
		
		//get docs in each group
		int  [] rows = new int [all_university_docs.size()];//total docs in each university
		int [] poses = new int [all_university_docs.size()];//index position for each university
		int sum = 0;
		for(int i=0;i<all_university_docs.size();i++)
		{
			rows[i] = all_university_docs.get(i).size();
			poses[i] = 0;
			sum += rows[i];
		}
		//roundrobin
		int index = 0;
		int added = 0;
		while(sum > 0)
		{
			if(poses[index] < rows[index])
			{
				lst.add(all_university_docs.get(index).get(poses[index]));
				poses[index]++;
				added++;
			}
			if(added == topk)
				break;
			index++;
			if(index == all_university_docs.size())
				index = 0;
			sum--;
		}
		//sort argain
		Collections.sort(lst);			
		return lst;	
	}

	void do_max_min_normalization(ArrayList<ArrayList<MyObject>> all_university_docs)
	{
		//normalize each university score by maximal value and minimal value
		for(int i=0;i<all_university_docs.size();i++)
		{
			double max_v = -1;
			double min_v = 100000;
			for(int j=0;j<all_university_docs.get(i).size();j++)
			{
				MyObject m = all_university_docs.get(i).get(j);
				if(m.value > max_v)
					max_v = m.value;
				if(m.value < min_v)
					min_v = m.value;
			}	
			//do normalization
			System.out.println("Maximal minimal " + max_v + " " + min_v);
			for(int j=0;j<all_university_docs.get(i).size();j++)
			{
				MyObject m = all_university_docs.get(i).get(j);
				double tmp = m.value;								
				m.value = (m.value - min_v) / (max_v - min_v);
				System.out.println(tmp + " " + m.value);
			}
			
		}
	}
	
	/**
	 * @param args
	 * @throws InterruptedException 
	 * @throws ParseException 
	 * @throws IOException 
	 */
	public void test(String[] args) throws InterruptedException, ParseException, IOException {
		// TODO Auto-generated method stub
		
		String unameParam = "uname";
		String ucountParam = "ucount";
		String mergeParam = "merge";
		String normParam = "norm";
		String topkParam = "topk";
		
		/*
		String uname_para = request.getParameter(unameParam);
		String ucount_para = request.getParameter(ucountParam);
		String merge_para = request.getParameter(mergeParam);
		String topk_para = request.getParameter(topkParam);
		String norm_para = request.getParameter(normParam);
		*/
		
		//String uname_para = "uwestern_uwaterloo_utoronto_ubrock";
		//String ucount_para = "10_10_10_10";
		String uname_para = "uwestern_uwaterloo_utoronto";
		String ucount_para = "10_10_10";
		String merge_para = null;
		String topk_para = "20";
		String norm_para = "yes";
		
		
		if(merge_para == null)
			merge_para = "roundrobin";
		
		if(topk_para == null)
			topk_para = "10";
		if(norm_para == null)
			norm_para = "yes";
		
		System.out.println(uname_para);
		System.out.println(ucount_para);
		System.out.println(merge_para);
		System.out.println(topk_para);
		System.out.println(norm_para);
		
		int topk = Integer.parseInt(topk_para);
		
		//get solr URL
		//String queryStr = request.getQueryString();
		//int a= queryStr.indexOf("solr=");
		//String solrs = queryStr.substring(a+5);
		String solrs = "http://kdd.csd.uwo.ca:88/seeu/servlet/forwardsolr/?q=machine%20learning&cur_cat=-1&hl=true&hl.fl=content%2Ctitle&hl.fragsize=200&json.nl=map&bq=org_26%3A1%5E2.0&defType=edismax&ps=0&tie=0.1&mm=2%3C-25%2525&qf=title%5E0.5%20content%20title_no_stemming%5E5%20content_no_stemming%5E5&pf=title%5E10%20content%5E5%20title_no_stemming%5E20%20content_no_stemming%5E10&pf2=title%5E20%20content%5E10%20title_no_stemming%5E40%20content_no_stemming%5E20&pf3=title%5E30%20content%5E15%20title_no_stemming%5E60%20content_no_stemming%5E30&boost=product(recip(ms(NOW%2FHOUR%2Clastmodified)%2C3.16e-11%2C1%2C1)%2Csum(boost%2C0.1)%2Cfactor_1)&wt=json&omitHeader=true";
		
		//System.out.println("Solr URL:" + solrs);				
		
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
		Hashtable<String, String> results = new Hashtable<String, String>();
		
		//call multiple thrashed to get HTTP content
		int limit = 20;
		BlockingQueue q = new ArrayBlockingQueue(limit);
		ThreadPoolExecutor es = new ThreadPoolExecutor(limit, limit, 20, TimeUnit.SECONDS, q);
		for(int i=0;i<uname_list.length;i++)
		{
			String uname = uname_list[i];
			
			//make solr query parameter
			String query_url = solrs + "&fq=uname:" + uname + "&rows=" + nd_ucount_list_list[i] + "&fl=id,url,title,snippet,score";
			
			es.execute(new HTTPGetContentThread("", uname, query_url, results) { /*  your task */ });
		}
		es.shutdown();
		boolean finshed = es.awaitTermination(1, TimeUnit.MINUTES);
						
		JSONParser parser=new JSONParser();
		//<id, score> pair
		ArrayList<ArrayList<MyObject>> all_university_docs = new ArrayList<ArrayList<MyObject>>();
		
		//<doc_id, object>
		Hashtable<String, JSONObject> all_doc_object = new Hashtable<String, JSONObject>();
		
		//<doc_id, object>
		Hashtable<String, JSONObject> all_highlight_object = new Hashtable<String, JSONObject>();
		
		//<uname, num found>
		Hashtable<String, Long> all_uname_numberfound = new Hashtable<String, Long>();
		int total_num_found = 0;
		for(int i=0;i<uname_list.length;i++)
		{
			String uname = uname_list[i];
						
			String content = results.get(uname);
			
			System.out.println(uname + "::" + content);
			//content = getContent(query_url);			
			if(content.equals(""))
			{
				//do nothing
			}
			else
			{
				//parse JSON object 				
				Object obj=parser.parse(content);
				JSONObject jObj = (JSONObject)obj;
				JSONObject current_response = (JSONObject) jObj.get("response");
				Long current_numfound = (Long) current_response.get("numFound");
				long num_found = current_numfound;
				
				System.out.println("match " + uname + " " + num_found);
				total_num_found += num_found;
				all_uname_numberfound.put(uname, num_found);
				
				//add docs into the whole set
				JSONArray current_docs = (JSONArray) current_response.get("docs");								
								
				//extract id and score for aggregation
				ArrayList<MyObject> tmp = new ArrayList<MyObject>();
				ArrayList<String> cur_ids = new ArrayList<String>();
				for(int k=0;k<current_docs.size();k++)
				{
					JSONObject cur_doc = (JSONObject)current_docs.get(k);
					String id = (String) cur_doc.get("id");
					cur_ids.add(id);
					double score = (Double) cur_doc.get("score");
					MyObject m = new MyObject();
					m.key = id;
					m.value = score;
					tmp.add(m);
					
					all_doc_object.put(id, cur_doc);
					//System.out.println(id + " " + score);
				}				
				all_university_docs.add(tmp);
				
				//extract the highlighting
				JSONObject current_highlight = (JSONObject) jObj.get("highlighting");												
				for(int k=0;k<cur_ids.size();k++)
				{
					JSONObject hightlight = (JSONObject)current_highlight.get(cur_ids.get(k));
					all_highlight_object.put(cur_ids.get(k), hightlight);
				}
			}								
		}
		
		System.out.println("SEE: get all docs" + all_doc_object.size() + "   "   + all_highlight_object.size());

		ArrayList<MyObject> lst = null;
		
		if(norm_para.equals("yes"))
			do_max_min_normalization(all_university_docs);
			
		if(merge_para.equals("simple"))
			lst = simple_merge(all_university_docs, topk);
		else if(merge_para.equals("roundrobin"))
			lst = roundrobin_merge(all_university_docs, topk);		
			
		for(int i=0;i<lst.size();i++)
		{		
			System.out.println("<p>"+i+" " + lst.get(i).key + " " + String.format("%.5f", lst.get(i).value) + "</p>");
		}
		
		//reassmbly the json output
		//build docs array by the sorted id list <lst>
		JSONArray final_docs = new JSONArray();
		JSONObject final_highlights = new JSONObject();		
		for(int i=0;i<lst.size();i++)
		{
			final_docs.add(all_doc_object.get(lst.get(i).key));
			final_highlights.put(lst.get(i).key, all_highlight_object.get(lst.get(i).key));			
		}
		//build highlight object
		JSONObject final_resposne = new JSONObject();
		final_resposne.put("numFound", total_num_found);
		final_resposne.put("docs", final_docs);				
		final_resposne.put("highlighting", final_highlights);
		JSONObject final_json = new JSONObject();
		final_json.put("response", final_resposne);
		StringWriter sw = new StringWriter();
		final_json.writeJSONString(sw);
		String jsonText = sw.toString();
		System.out.println(jsonText);
	}
	
	public static void main(String[] args) throws InterruptedException, ParseException, IOException {
		TestRankAggregation t = new TestRankAggregation();
		t.test(args);
	}
	
}
