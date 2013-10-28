package ca.uwo.seeu.hadoop;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import com.mysql.jdbc.Statement;

public class SEEUExtractAnchorText {

	public static class TokenizerMapper extends
			Mapper<Object, Text, Text, Text> {

		private final static IntWritable one = new IntWritable(1);
		private Text url = new Text();
		private Text did = new Text();
		private Text anchor = new Text();
		private Connection conn = null;
		private Statement stmt = null;
		private String schema = "trec";
		private String html_tb = "webtrack_2009";
		private String url_tb = "webtrack_2009_text_feature";
		private String dburl = "jdbc:mysql://192.168.0.2:3306/";
		private String userName = "root";
		private String password = "see";

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

		public String readDB(String id) throws IOException, SQLException,
				InstantiationException, IllegalAccessException,
				ClassNotFoundException {
			String ret = "";

			String sql = "select html from " + this.schema + "." + this.html_tb
					+ " where id =\"" + id + "\"";
			System.out.println(sql);
			ResultSet rs = null;
			try {
				rs = stmt.executeQuery(sql);
			} catch (Exception ex) {
				// close database connection
				System.out
						.println("Database Exception. Try to reset connection");
				close_db();
				// establish the link to the database again
				init_db_connection();
			}
			// Loop through the result set
			while (rs.next()) {
				// get doc ID, this is a digit number read from DB
				ret = rs.getString(1);
			}

			rs.close();

			return ret;
		}

		public String readURLDB(String id) throws IOException, SQLException,
				InstantiationException, IllegalAccessException,
				ClassNotFoundException {
			String ret = "";

			String sql = "select url from " + this.schema + "." + this.url_tb
					+ " where id =\"" + id + "\"";
			System.out.println(sql);
			ResultSet rs = null;
			try {
				rs = stmt.executeQuery(sql);
			} catch (Exception ex) {
				// close database connection
				System.out
						.println("Database Exception. Try to reset connection");
				close_db();
				// establish the link to the database again
				init_db_connection();
			}
			// Loop through the result set
			while (rs.next()) {
				// get doc ID, this is a digit number read from DB
				ret = rs.getString(1);
			}

			rs.close();

			return ret;
		}

		void extract_all_links(String baseurl, String html,
				ArrayList<String> all_links, ArrayList<String> all_link_text) {
			try {
				Document doc = Jsoup.parse(html, baseurl);
				Elements links = doc.select("a[href]");
				for (Element link : links) {
					String url = link.attr("abs:href");
					String text = link.text();
					all_links.add(url);
					all_link_text.add(text);
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		public String remove_newline_string(String s) {
			String ret = s;
			ret = ret.replaceAll("\n\r", " ");
			ret = ret.replaceAll("\r", " ");
			ret = ret.replaceAll("\n", " ");
			return ret;
		}

		public String repair_html(String doc) {
			int first_html = doc.indexOf("<html>");
			String whole_doc = doc.substring(first_html + 6);
			int second_html = whole_doc.indexOf("<html>");
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

		public void query_html(ArrayList<String> subList,
				Hashtable<String, String> docs) throws InstantiationException,
				IllegalAccessException, ClassNotFoundException, SQLException,
				IOException {
			String sql = "select id,html from " + this.schema + "."
					+ this.html_tb + " where id in (" + join(subList, ",")
					+ ")";
			//System.out.println(sql);
			ResultSet rs = null;
			try {
				rs = stmt.executeQuery(sql);
			} catch (Exception ex) {
				// close database connection
				System.out
						.println("Database Exception. Try to reset connection");
			}
			// Loop through the result set
			while (rs.next()) {
				// get doc ID, this is a digit number read from DB
				String id = rs.getString(1);
				String html = rs.getString(2);

				docs.put(id, html);
			}

			rs.close();
		}

		public void query_urls(ArrayList<String> subList,
				Hashtable<String, String> docs) throws InstantiationException,
				IllegalAccessException, ClassNotFoundException, SQLException,
				IOException {
			String sql = "select id,url from " + this.schema + "."
					+ this.url_tb + " where id in (" + join(subList, ",")
					+ ")";
			//System.out.println(sql);
			ResultSet rs = null;
			try {
				rs = stmt.executeQuery(sql);
			} catch (Exception ex) {
				// close database connection
				System.out
						.println("Database Exception. Try to reset connection");
			}
			// Loop through the result set
			while (rs.next()) {
				// get doc ID, this is a digit number read from DB
				String id = rs.getString(1);
				String html = rs.getString(2);

				docs.put(id, html);
			}

			rs.close();
		}
		
		public void string_to_id_list(String in, ArrayList<String> lst) {
			String[] id_array = in.split(";");
			for (int i = 0; i < id_array.length; i++) {
				lst.add("\"" + id_array[i] + "\"");
			}
		}

		
		
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			
			Configuration conf = context.getConfiguration();
			schema = conf.get("db_schema");
			html_tb = conf.get("html_tb");
			url_tb = conf.get("url_tb");
			String idurlmap = conf.get("idurlmap");
			
			Hashtable<String, String> url2id = new Hashtable<String, String>();
			read_url_to_id(idurlmap, url2id);
			
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

			// value will be a doc id list
			StringTokenizer itr = new StringTokenizer(value.toString());
			int n_doc = 0;
			while (itr.hasMoreTokens()) {
				String str_id_list = itr.nextToken();
				
				ArrayList<String> ids = new ArrayList<String>();
				this.string_to_id_list(str_id_list, ids);
				
				Hashtable<String, String> docs = new Hashtable<String, String>();
				try {
					this.query_html(ids, docs);
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
				
				Hashtable<String, String> urls = new Hashtable<String, String>();
				try {
					this.query_urls(ids, urls);
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
				System.out.println(ids.size());
				System.out.println(docs.size());
				System.out.println(urls.size());
				
				String[] id_array = str_id_list.split(";");
				for(int k=0;k<id_array.length;k++)
				{
					String id = id_array[k];
					String html = docs.get(id);
					String base_url = urls.get(id);	
					
					if(html != null && html.equals("") == false && base_url != null)
					{
						ArrayList<String> all_links = new ArrayList<String>();
						ArrayList<String> all_link_text = new ArrayList<String>();
						extract_all_links(base_url, html, all_links, all_link_text);
						// output <url, text>
						int m_out = 0;
						for (int i = 0; i < all_links.size(); i++) {
							String turl = all_links.get(i);
							String did = url2id.get(turl);
							if(did != null)
							{
								this.did.set(did);
								String an = remove_newline_string(all_link_text.get(i));
								anchor.set(id + "[[]]" + an);//(baseurl, text)
								m_out+=1;
								context.write(this.did, anchor);
							}
						}
						System.out.println(id + " " + all_links.size() + " done " + m_out);
					}
					
					n_doc += 1;
				}
				
				docs.clear();
				urls.clear();
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
	}

	public static class IntSumReducer extends Reducer<Text, Text, Text, Text> {
		private Text result = new Text();
		private Text url = new Text();

		// <url, <text>+> -> <url, text>
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			String str_key = key.toString();
			if(str_key.equals(""))
				return;
			
			StringBuilder sb = new StringBuilder();
			int first = 0;
			for (Text val : values) {
				if (first == 0) {
					sb.append(val.toString());
					first = 1;
				} else
					sb.append("||||" + val.toString());
			}
			url.set(key);
			result.set(sb.toString());
			context.write(url, result);
		}
	}

	public static void read_url_to_id(String fname, Hashtable<String, String> map) throws IOException
	{
		BufferedReader br = new BufferedReader(new FileReader(fname));
		String line = "";
		while((line = br.readLine()) != null)
		{
			int a = line.indexOf(" ");
			String did = line.substring(0, a);
			String url = line.substring(a+1);
			map.put(url, did);
		}
		br.close();
	}
	
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 12) {			
		      System.err.println("Usage: Extract [-idurlmap <idurlmap>] [-schema <db_schema>] [-html_tb <html_tb>] [-url_tb <url_tb>] [-idfolder <id_folder>] [-outfolder <outfolder>]");
		      return ;
		    }		  
		  String db_schema = "";		  
		  String idfolder = "";		  		  
		  String html_tb = "";
		  String url_tb = "";
		  String outfolder = "";
		  String idurlmap = "";
		// TODO Auto-generated method stub
		    for (int i = 0; i < otherArgs.length; i++) {
		    	if (otherArgs[i].equals("-schema")) {
		    	  db_schema = otherArgs[++i];
		      }else if (otherArgs[i].equals("-html_tb")) {
		    	  html_tb = otherArgs[++i];
		      }else if (otherArgs[i].equals("-url_tb")) {
		    	  url_tb = otherArgs[++i];
		      }	      
		      else if (otherArgs[i].equals("-idfolder")) {
		    	  idfolder = otherArgs[++i];
		      }			      
		      else if (otherArgs[i].equals("-outfolder")) {
		    	  outfolder = otherArgs[++i];
		      }
		      else if (otherArgs[i].equals("-idurlmap")) {
		    	  idurlmap = otherArgs[++i];
		      }
		    }
		    
		conf.set("mapred.job.tracker", "local");
		conf.set("fs.default.name", "file:///");

		conf.set("mapred.textoutputformat.separator", "<<>>");
		conf.set("db_schema", db_schema);
		conf.set("html_tb", html_tb);
		conf.set("url_tb", url_tb);		
		conf.set("idurlmap", idurlmap);
		
		//Hashtable<String, String> map1 = new Hashtable<String, String> ();
		//read_url_to_id(idurlmap,map1 );
		//System.out.println(map1.size());
		
		Job job = new Job(conf, "seeu classification");
		job.setJarByClass(SEEUExtractAnchorText.class);
		job.setMapperClass(TokenizerMapper.class);

		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(idfolder));
		FileOutputFormat.setOutputPath(job, new Path(outfolder));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
		
	}
}
