package ca.uwo.seeu.bayes;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.html.HtmlParser;
import org.apache.tika.sax.BodyContentHandler;
import org.apache.tika.sax.Link;
import org.apache.tika.sax.LinkContentHandler;
import org.apache.tika.sax.TeeContentHandler;
import org.apache.tika.sax.ToHTMLContentHandler;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.xml.sax.ContentHandler;

import com.mysql.jdbc.Statement;

public class WordClassCountReducer 
		extends Reducer<Text,IntWritable,Text,IntWritable> {
		//extends Reducer<Text, Text, Text, Text> {

		private final static IntWritable one = new IntWritable(1);
		private Text url = new Text();
		private Text did = new Text();
		private Text anchor = new Text();
		private Text report = new Text();
		private Text result = new Text();
		private IntWritable sumresult = new IntWritable();
		private Text out_text = new Text();
		private Connection conn = null;
		private Statement stmt = null;
		private String schema = "trec";
		private String html_tb = "webtrack_2009";
		private String text_tb = "webtrack_2009_text_feature";
		private String dburl = "jdbc:mysql://192.168.0.2:3306/";
		private String userName = "root";
		private String password = "see";
		private int chunk_size = 100;
		
		private Hashtable<String, String> url2id = null;
		
		public void init_db_connection() throws InstantiationException,
				IllegalAccessException, ClassNotFoundException, SQLException {
			Class.forName("com.mysql.jdbc.Driver").newInstance();
			conn = DriverManager.getConnection(dburl, userName, password);
			stmt = (Statement) conn.createStatement();

			System.out.println("Connecting to " + dburl);
			System.out.println("Database connection established");
		}

		public void close_db() throws IOException, SQLException,
				InstantiationException, IllegalAccessException,
				ClassNotFoundException {
			if (stmt != null) {
				try {
					stmt.close();
					System.out.println("Database statement terminated");
				} catch (Exception e) {
					System.err.println(e);
				}
			}
			if (conn != null) {
				try {
					conn.close();
					System.out.println("Database connection terminated");
				} catch (Exception e) {
					System.err.println(e);
				}
			}
		}

		public void insert_anchor_text(String did, ArrayList<String> link_list, ArrayList<String> text_list) throws InstantiationException,
		IllegalAccessException, ClassNotFoundException, SQLException,
		IOException {
			try {
				init_db_connection();
			} catch (InstantiationException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			} catch (IllegalAccessException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			} catch (ClassNotFoundException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			} catch (SQLException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
									
			String sql = "update " + this.schema + "."
					+ this.text_tb 
					+ " set in_link_ids=?, in_text_list=?, number_in_links=?, all_anchor_text=? "
					+ " where id=?";
			
			try
			{
				PreparedStatement pStmt = conn.prepareStatement(sql);
				
				String out_link_ids = join(link_list, "<<>>");
				String out_link_text = join(text_list, "<<>>");
				String all_anchor_text = join(text_list, " ");
				int number_in_links = link_list.size();
				
				pStmt.setString(1, out_link_ids);			
				pStmt.setString(2, out_link_text);
				pStmt.setString(3, ""+number_in_links);
				pStmt.setString(4, all_anchor_text);
				pStmt.setString(5, did);				
				
				pStmt.executeUpdate();
				
				System.out.println(out_link_ids + "<<>>" + out_link_text + "<<>>" + did);
				
				System.out.println(did + " update to db");
			}
			catch(Exception e)
			{
				e.printStackTrace();
			}
		
			
			try {
				close_db();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InstantiationException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IllegalAccessException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		
		/*
		// <url, <text>+> -> <url, text>
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			String str_key = key.toString();
			if(str_key.equals(""))
				return;
			
			//assembly in links
			//assembly in text
			ArrayList<String> inIds = new ArrayList<String>();
			ArrayList<String> inTexts = new ArrayList<String>();			
			for (Text val : values) {
				String v = val.toString();				
				String [] v2 = v.split("<<>>");
				if (v2.length >= 2)
				{
					inIds.add(v2[0]);
					inTexts.add(v2[1]);
				}
				else
				{
					inIds.add(v2[0]);
					inTexts.add("");
				}
			}
			
			Configuration conf = context.getConfiguration();
			this.schema = conf.get("db_schema");
			this.text_tb = conf.get("text_tb");
					
			try {
				insert_anchor_text(str_key, inIds, inTexts);
			} catch (InstantiationException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IllegalAccessException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		*/
		
		public String remove_newline_string(String s) {
			String ret = s;
			ret = ret.replaceAll("\n\r", " ");
			ret = ret.replaceAll("\r", " ");
			ret = ret.replaceAll("\n", " ");
			return ret;
		}

		public String repair_html(String doc) {
			String lowerDoc = doc.toLowerCase();
			int first_html = lowerDoc.indexOf("<html>");			
			String whole_doc = doc.substring(first_html + 6);
			String lower_whole_doc = lowerDoc.substring(first_html + 6);
			
			int second_html = lower_whole_doc.indexOf("<html");			
			String html_doc = whole_doc.substring(second_html);
			
			int end_pre = html_doc.lastIndexOf("</PRE>");
			String new_html_doc = "";
			if (end_pre == -1)
				new_html_doc = html_doc;
			else {
				new_html_doc = html_doc.substring(0, end_pre);
			}
			return new_html_doc;
		}

		public static String join(Collection s, String delimiter) {
			StringBuffer buffer = new StringBuffer();
			Iterator iter = s.iterator();
			while (iter.hasNext()) {
				buffer.append(iter.next());
				if (iter.hasNext()) {
					buffer.append(delimiter);
				}
			}
			return buffer.toString();
		}

		public void query_html(String keytype, List<String> subList,
				Hashtable<String, String> docs) throws InstantiationException,
				IllegalAccessException, ClassNotFoundException, SQLException,
				IOException {
			try {
				init_db_connection();
			} catch (InstantiationException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			} catch (IllegalAccessException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			} catch (ClassNotFoundException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			} catch (SQLException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
						
			String sql = "select id,url,html from " + this.schema + "."
					+ this.html_tb + " where id in (" + join(subList, ",")
					+ ")";
			//System.out.println(sql);
			ResultSet rs = null;
			try {
				rs = stmt.executeQuery(sql);
			} catch (Exception ex) {
				// close database connection
				System.out.println("Database Exception. Try to reset connection");
				ex.printStackTrace();
			}
			// Loop through the result set
			while (rs.next()) {
				// get doc ID, this is a digit number read from DB
				String id = rs.getString(1);
				if(keytype.equals("string"))
					id = "\"" + id + "\"";//append string quote
				String url = rs.getString(2);
				String html = rs.getString(3);

				docs.put(id, url+"<<>>"+html);
			}
			if(rs != null)
				rs.close();
			
			try {
				close_db();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InstantiationException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IllegalAccessException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		public void write_text(Hashtable<String, Hashtable<String, String>> docs, Context context) throws IOException, InterruptedException
		{
			Set<String> ids = docs.keySet();
			Iterator<String> it = ids.iterator();
			while(it.hasNext())
			{				
				String did = it.next();
				Hashtable<String, String> html_feature = docs.get(did);
				String url_text = html_feature.get("url");
				String title_text = html_feature.get("title");
				String desc_text = html_feature.get("description");
				String keywords_text = html_feature.get("keywords");
				String body_text = html_feature.get("body");
				String anchor_text = "";	
				String whole_text = html_feature.get("whole");				
				
				this.did.set(did);
				this.out_text.set(url_text + "<<>>" + title_text  + "<<>>" + desc_text  + "<<>>" + keywords_text  + "<<>>" + body_text  + "<<>>" + anchor_text   + "<<>>" + whole_text);
				//context.write(this.did, this.out_text);
				
				System.out.println(did + " insert to db");			
			}
		}
		
		public void insert_text(Hashtable<String, Hashtable<String, String>> docs) throws InstantiationException,
				IllegalAccessException, ClassNotFoundException, SQLException,
				IOException {
			try {
				init_db_connection();
			} catch (InstantiationException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			} catch (IllegalAccessException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			} catch (ClassNotFoundException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			} catch (SQLException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			
			Set<String> ids = docs.keySet();
			Iterator<String> it = ids.iterator();
			while(it.hasNext())
			{
				String did = it.next();
				String sql = "insert into " + this.schema + "."
						+ this.text_tb 
						+ " (id, url_text, title_text, desc_text, keyword_text, body_text, anchor_text, whole_text) "
						+ " values (?,?,?,?,?,?,?,?)";
				
				try
				{
					PreparedStatement pStmt = conn.prepareStatement(sql);
					
					Hashtable<String, String> html_feature = docs.get(did);
					String url_text = html_feature.get("url");
					String title_text = html_feature.get("title");
					String desc_text = html_feature.get("description");
					String keywords_text = html_feature.get("keywords");
					String body_text = html_feature.get("body");
					String anchor_text = "";	
					String whole_text = html_feature.get("whole");				
					
					pStmt.setString(1, did);				
					pStmt.setString(2, url_text);
					pStmt.setString(3, title_text);
					pStmt.setString(4, desc_text);
					pStmt.setString(5, keywords_text);
					pStmt.setString(6, body_text);
					pStmt.setString(7, anchor_text);
					pStmt.setString(8, whole_text);
					
					pStmt.executeUpdate();
					
					System.out.println(did + " insert to db");
				}
				catch(Exception e)
				{
					e.printStackTrace();
				}
			}
			
			try {
				close_db();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InstantiationException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IllegalAccessException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		public void string_to_id_list(String keytype, String in, ArrayList<String> lst) {
			String[] id_array = in.split(";");
			for (int i = 0; i < id_array.length; i++) {
				if(keytype.equals("string"))					
					lst.add("\"" + id_array[i] + "\"");
				else if(keytype.equals("int"))					
					lst.add(id_array[i]);
			}
		}
		
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
		 int sum = 0;
	      for (IntWritable val : values) {
	        sum += val.get();
	      }
	      sumresult.set(sum);
	      context.write(key, sumresult);
	    }
							
		
		public void doJob(int debug, Context context, String url2id_fname, String db_schema, String html_tb, String text_tb, String idLine, String keytype, int chunk_size, boolean isClueweb09) throws IOException, InterruptedException
		{
			this.schema = db_schema;
			this.html_tb = html_tb;
			this.text_tb = text_tb;									
			this.chunk_size = chunk_size;
			
			//read URL2id map					
			this.readURLtoIDmapping(url2id_fname, url2id);			
			
			// value will be a doc id list
			//it is assumed that the id files is composed of lines as
			//id;id;id;id;id;id;id;id;id;id;
			StringTokenizer itr = new StringTokenizer(idLine);
			
			String work_status = "Do mapper job";
			if(debug == 0)
				context.setStatus(work_status);
			
			while (itr.hasMoreTokens()) {
				String str_id_list = itr.nextToken();
				
				ArrayList<String> all_ids = new ArrayList<String>();
				this.string_to_id_list(keytype, str_id_list, all_ids);
				
				//split id by chunk size
				ArrayList<List<String>> chunks = new ArrayList<List<String>>();
				split_id_lst(all_ids, chunks, chunk_size);
				
				for(int chunk_id=0;chunk_id<chunks.size();chunk_id++)
				{				  
					List<String> ids = chunks.get(chunk_id);
					
					Hashtable<String, String> docs = new Hashtable<String, String>();
					try {
						long t_start = System.currentTimeMillis();
						System.out.println("Start querying "+ids.size()+" docs");
						
						if(debug == 0)
							context.setStatus("Start querying "+ids.size()+" docs");
						
						//query DB tp read <id, url, html>
						this.query_html(keytype, ids, docs);
						
						if(debug == 0)
							context.setStatus("Finish querying "+docs.size()+" docs");
						
						long t_end = System.currentTimeMillis();
						System.out.println("Actually querying "+docs.size()+" docs in "+(t_end-t_start)/1000+" seconds");
					} catch (InstantiationException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (IllegalAccessException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (ClassNotFoundException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (SQLException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
								
					Hashtable<String, Hashtable<String, String>> textList = new Hashtable<String, Hashtable<String, String>>();					
					for(int k=0;k<ids.size();k++)
					{
						String id = ids.get(k);						
						String url_html = docs.get(id);
						if(url_html == null)
							continue;
						
						int p = url_html.indexOf("<<>>");
						
						String base_url = url_html.substring(0, p);
						String html = url_html.substring(p+4);					
						
						if(html != null && html.equals("") == false && base_url != null && base_url.equals("") == false)
						{
							String htmlDoc = html;
							if(isClueweb09)
								htmlDoc = this.repair_html(html);//repair cluweb09 html form
							Hashtable<String, String> data = new Hashtable<String, String>();
							try {
								if(debug == 0)
									context.setStatus("Start extracting "+id+" doc");
								
								//extract <outgoing_link, acnchor_text> 
								extractSEEUAnchorFeatures(context, id, htmlDoc, base_url, url2id, data);
								
								if(debug == 0)
									context.setStatus("Finish extracting "+id+" doc");
								//this.did.set(id);
								//this.report.set("done");
								//context.write(this.did, this.report);
								textList.put(id, data);
							} catch (SQLException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							} catch (InstantiationException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							} catch (IllegalAccessException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							} catch (ClassNotFoundException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}												
						}					
					}					
					
					//clear cache
					textList.clear();
					docs.clear();	
				}
			}			
		}
		
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			
			Configuration conf = context.getConfiguration();			
			doJob(0, context, conf.get("seeu_url2id_fname"), conf.get("db_schema"), conf.get("html_tb"), conf.get("text_tb"), value.toString(), conf.get("key_type"), Integer.parseInt(conf.get("chunk_size")), false);
		}
		
		public String extractFeature(String url)
		{
			StringBuilder ret = new StringBuilder();
			Pattern pattern = Pattern.compile("\\w+");
		    // In case you would like to ignore case sensitivity you could use this
		    // statement
		    // Pattern pattern = Pattern.compile("\\s+", Pattern.CASE_INSENSITIVE);
		    Matcher matcher = pattern.matcher(url);
		    // Check all occurance
		    boolean first = true;
		    while (matcher.find()) {
		    	if(first)
		    	{
		    		ret.append(matcher.group());
		    		ret.append(" ");
		    		first = false;
		    	}
		    	else
		    	{
		    		ret.append(matcher.group());
		    		ret.append(" ");
		    	}
		    }			    		  
			return ret.toString();
		}
		
		/*
		 * baseurl: base URL
		 * html: HTML source
		 * all_links: all URL links
		 * all_link_text: the anchor text inside each links
		 */
		void extractAnchorFeatureByTika(String baseurl, String html,
				ArrayList<String> all_links, ArrayList<String> all_link_text) {
			try {
				LinkContentHandler linkHandler = new LinkContentHandler();
		        ContentHandler textHandler = new BodyContentHandler(-1);
		        ToHTMLContentHandler toHTMLHandler = new ToHTMLContentHandler();
		        TeeContentHandler teeHandler = new TeeContentHandler(linkHandler, textHandler, toHTMLHandler);
		        
		        Metadata metadata = new Metadata();
		        ParseContext parseContext = new ParseContext();
		        HtmlParser parser = new HtmlParser();
		        
		        InputStream input = IOUtils.toInputStream(html);
		        parser.parse(input, teeHandler, metadata, parseContext);
		        /*
		        String title = metadata.get("title");
		        String keywords = metadata.get("keywords");
		        String description = metadata.get("description");
		        String body = textHandler.toString();
		        
		        if(title == null)
		        	title = "";
		        if(keywords == null)
		        	keywords = "";
		        if(description == null)
		        	description = "";
		        if(body == null)
		        	body = "";
		    		       
				//extract text 
				title = extractFeature(title);								
				
				description = extractFeature(description);
				
				keywords = extractFeature(keywords);
				
				body = extractFeature(body);
				
				if(tolower)
				{
					title = title.toLowerCase();
					description = description.toLowerCase();
					keywords = keywords.toLowerCase();
					body = body.toLowerCase();
				}
				
				data.put("title", title);
				data.put("description", description);
				data.put("keywords", keywords);
				data.put("body", body);	
				*/
				
		        //extract all links
		        List<Link> links = linkHandler.getLinks();
		        URL baseURL = new URL(baseurl);
		        for(Link l : links)
		        {
		        	URL newURL = new URL(baseURL, l.getUri());
		        	if(l.isAnchor())
		        	{
		        		all_links.add(newURL.toString());
		        		all_link_text.add(l.getText());
		        	}		        	
		        } 
		        
				input.close();
				
			} catch (Exception e) {
				e.printStackTrace();
			}
			catch(Error e)
			{
				e.printStackTrace();
			}
		}
		
		/*
		 * html: the HTML source code
		 * tolower: if change the text to lowercase
		 */
		public void extractTextFeatureByTika(String html, boolean tolower, Hashtable<String, String> data)
		{
			try {
				LinkContentHandler linkHandler = new LinkContentHandler();
		        ContentHandler textHandler = new BodyContentHandler(-1);
		        ToHTMLContentHandler toHTMLHandler = new ToHTMLContentHandler();
		        TeeContentHandler teeHandler = new TeeContentHandler(linkHandler, textHandler, toHTMLHandler);
		        
		        Metadata metadata = new Metadata();
		        ParseContext parseContext = new ParseContext();
		        HtmlParser parser = new HtmlParser();
		        
		        InputStream input = IOUtils.toInputStream(html);
		        parser.parse(input, teeHandler, metadata, parseContext);
		        
		        String title = metadata.get("title");
		        String keywords = metadata.get("keywords");
		        String description = metadata.get("description");
		        String body = textHandler.toString();
		        
		        if(title == null)
		        	title = "";
		        if(keywords == null)
		        	keywords = "";
		        if(description == null)
		        	description = "";
		        if(body == null)
		        	body = "";
		    		       
				//extract text 
				title = extractFeature(title);								
				
				description = extractFeature(description);
				
				keywords = extractFeature(keywords);
				
				body = extractFeature(body);
				
				if(tolower)
				{
					title = title.toLowerCase();
					description = description.toLowerCase();
					keywords = keywords.toLowerCase();
					body = body.toLowerCase();
				}
				
				data.put("title", title);
				data.put("description", description);
				data.put("keywords", keywords);
				data.put("body", body);	
				
				input.close();
				
			} catch (Exception e) {
				e.printStackTrace();
			}
			catch(Error e)
			{
				e.printStackTrace();
			}
		}				
		
		public void extractSEEUAnchorFeatures(Context context, String id, String htmlDoc, String baseurl, Hashtable<String, String> url2id, Hashtable<String, String> data) throws IOException, SQLException, InstantiationException, IllegalAccessException, ClassNotFoundException, InterruptedException
		{			  
			  //repair html from clueweb data	
			  ArrayList<String> all_links = new ArrayList<String>(); 
			  ArrayList<String> all_link_text = new ArrayList<String>();
			  
			  extractAnchorFeatureByTika(baseurl, htmlDoc, all_links, all_link_text);
				
			// output <ID, anchor>
				int m_out = 0;
				for (int i = 0; i < all_links.size(); i++) {
					String turl = all_links.get(i);
					String did = url2id.get(turl);
					if(did != null)
					{
						this.did.set(did);
						//get anchor text
						String anchor_text = remove_newline_string(all_link_text.get(i));
						this.anchor.set(id + "[[]]" + anchor_text);//(toid, text)
						m_out+=1;
						//context.write(this.did, this.anchor);
					}
				}
				System.out.println(id + " " + all_links.size() + " done " + m_out);		  
		}
		
		public void readIDList(String idFname, ArrayList<String> idList) throws IOException
		{
			//read all ids	    	
	    	BufferedReader br = new BufferedReader(new FileReader(idFname));
	    	String line = null;
	    	while((line = br.readLine()) != null)
	    	{
	    		idList.add(line);
	    	}
	    	br.close();
		}
		
		public static void test() throws Exception {
						
		  WordClassCountReducer  h = new WordClassCountReducer();
		  
		  ArrayList<String> idList = new ArrayList<String>();
		  h.readIDList("/media/DataVolume1/datasets/trec_web_track/2009/c", idList);
		  h.init_db_connection();
		  
		  for(int i=0;i<idList.size() && i< 1;i++)
		  {
			  String line = idList.get(i);
			  int p = line.indexOf(" ");
			  String did = line.substring(0, p);
			  String url = line.substring(p+1);			  
			  Hashtable<String, String> data = new Hashtable<String, String>();
			  //h.extractSEEUFeatures(did, url, data);
			  /*
			  String [] all_fields = {"url", "title", "description", "keywords", "body", "whole"};			  
			  for(int j=0;j<all_fields.length;j++)
			  {			  
				  System.out.println(all_fields[j] + " " + data.get(all_fields[j]));
			  }
			  */
			  System.out.println(did + " " + data.get("title") + " " + data.get("url"));
		  }
		  
		  h.close_db();		 
		}
		
		public void split_id_lst(ArrayList<String> ids, ArrayList<List<String>> chunks, int chunk_size)
		{						
			int i=0;
			int nDocs = ids.size();
			for(i=0;i<nDocs;i+=chunk_size)
			{
				//build a small trunk
				int to = (i+chunk_size > nDocs)?nDocs:i+chunk_size; 
				List<String> subList = ids.subList(i, to);
				chunks.add(subList);				
			}
		}
		
		public void readURLtoIDmapping(String fname, Hashtable<String, String> url2id) throws IOException
		{
			BufferedReader br = new BufferedReader(new FileReader(fname));
			String line = "";
			StringBuilder content = new StringBuilder();
			while((line = br.readLine()) != null)
			{
				String [] fields = line.split("<<>>");
				url2id.put(fields[1], fields[0]);
			}
			br.close();
		}
		
		public static void main(String[] args) throws Exception {
			
			 // test();
			
			  WordClassCountReducer m = new WordClassCountReducer();
			  //String idLine = "15";
			  String idLine = "";
			  String fname = "/home/xiao/workspace/HadoopProject/data/query_bing/a";
			  BufferedReader br = new BufferedReader(new FileReader(fname));
			  String l = "";
			  while((l = br.readLine()) != null)
			  {
				  idLine += l + ";";
			  }
			  br.close();
			  
			  idLine = "504792";
			  long t_start = System.currentTimeMillis();			 
			  m.doJob(1, null, "/home/mpi/shareddir/seeu/query_bing_id_url_list.txt", "query_search_engine", "webdoc", "seeu_anchor_text_feature", idLine, "int", 100, false);			 
			  long t_end = System.currentTimeMillis();
			  System.out.println((t_end-t_start)/1000 + "seconds");			
			  /*
			String a = "12<<>>";
			String [] b = a.split("<<>>");
			System.out.println(b[0] + "||" + b[1]);
			*/
		}
		
	}