package ca.uwo.seeu.bayes;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
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
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import com.mysql.jdbc.Statement;

public class WordClassCountMapper extends
			//Mapper<Object, Text, Text, Text> {
			Mapper<Object, Text, Text, IntWritable>{

		private final static IntWritable count = new IntWritable(0);		
		private Text key = new Text();
		private Text value = new Text();				
		private Connection conn = null;
		private Statement stmt = null;
		private String schema = "trec";
		private String feature_tb = "webpages";		
		private String dburl = "jdbc:mysql://192.168.0.2:3306/";
		private String userName = "root";
		private String password = "see";
		private int chunk_size = 100;				
		
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

		public String remove_newline_string(String s) {
			String ret = s;
			ret = ret.replaceAll("\n\r", " ");
			ret = ret.replaceAll("\r", " ");
			ret = ret.replaceAll("\n", " ");
			return ret;
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

		public void query_data(String keytype, List<String> subList,
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
						
			String sql = "select id,svm_feature,labels from " + this.schema + "."
					+ this.feature_tb + " where id in (" + join(subList, ",")
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
				String features = rs.getString(2);
				String labels = rs.getString(3);

				docs.put(id, features+"<<>>"+labels);
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
	
		public void string_to_id_list(String keytype, String in, ArrayList<String> lst) {
			String[] id_array = in.split(";");
			for (int i = 0; i < id_array.length; i++) {
				if(keytype.equals("string"))					
					lst.add("\"" + id_array[i] + "\"");
				else if(keytype.equals("int"))					
					lst.add(id_array[i]);
			}
		}
		
		public void doJob(int debug, Context context, 
				String db_schema, String dataset_tb, 
				String idLine, 
				String keytype, int chunk_size) throws IOException, InterruptedException
		{
			this.schema = db_schema;
			this.feature_tb = dataset_tb;										
			this.chunk_size = chunk_size;
					
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
						
						//query DB tp read <id, features, labels>
						this.query_data(keytype, ids, docs);
						
						if(debug == 0)
							context.setStatus("Finish querying "+docs.size()+" docs");

						//extract word-class pair
						for(int i=0;i<ids.size();i++)
						{
							String did = ids.get(i);
							String field = docs.get(did);
							String [] field_values = field.split("<<>>");
							String feautre = field_values[0];
							String labels = field_values[1];
							
							String [] label_values = labels.split(" ");
							String [] feautre_values = feautre.split(" ");

							//for each cat, out put the <cat_word, count>
							//format is <did, num_of_cat, cat+>
							int cat_count = Integer.parseInt(label_values[1]);
							if(cat_count > 0)
							{
								//for each category
								for(int j=2;j<label_values.length;j++)
								{
									String cat = label_values[j];
									
									//for each word
									//iterat each word 
									//this format is <did, word+>
									for(int k=1;k<feautre_values.length;k++)
									{
										String [] word_value = feautre_values[k].split(":");
										String word = word_value[0];
										int occ = Integer.parseInt(word_value[1]);
										
										//emit this pair
										//<class_word, count>
										this.key.set(cat + '_' + word);
										//this.value.set(""+occ);
										this.count.set(occ);
										context.write(this.key, this.count);
										System.out.println(this.key + " " + this.count);
									}
								}
							}
						}
												
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
					docs.clear();	
				}
			}			
		}
		
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			
			Configuration conf = context.getConfiguration();
			
			doJob(0, context, conf.get("db_schema"), conf.get("dataset_tb"), value.toString(), conf.get("key_type"), Integer.parseInt(conf.get("chunk_size")));
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
						
		  WordClassCountMapper  h = new WordClassCountMapper();
		  
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
			while((line = br.readLine()) != null)
			{
				String [] fields = line.split("<<>>");
				url2id.put(fields[1], fields[0]);
			}
			br.close();
		}
		
		public static void main(String[] args) throws Exception {
			
			 // test();			 
			  WordClassCountMapper m = new WordClassCountMapper();
			  //String idLine = "15";
			  String idLine = "";
			  String fname = "/media/DataVolume1/datasets/see/a";
			  BufferedReader br = new BufferedReader(new FileReader(fname));
			  String l = "";
			  while((l = br.readLine()) != null)
			  {
				  idLine += l + ";";
			  }
			  br.close();
			  
			  idLine = "1";
			  long t_start = System.currentTimeMillis();			 
			  m.doJob(1, null, "see", "webpages", idLine, "int", 100);			 
			  long t_end = System.currentTimeMillis();
			  System.out.println((t_end-t_start)/1000 + "seconds");			
			
			
		}
		
	}