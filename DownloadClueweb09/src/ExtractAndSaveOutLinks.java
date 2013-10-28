import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.ProtocolException;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import com.mysql.jdbc.Statement;

public class ExtractAndSaveOutLinks {

	private Connection conn = null;	
	private Statement stmt = null ;
	public String schema = "";
	public String db = "";
	public String html_tb = "";
	public String text_tb = "";
	private String dburl = "jdbc:mysql://192.168.0.2:3306/";
	private String userName = "root";
	private String password = "see";
	private int commitWithin = 300000;//5 mins
    public int querySize = 50;
    private int commitSize = 50;
    private Hashtable<String, StringBuilder> docs = new Hashtable<String, StringBuilder>();
    private ArrayList<String> docIDs = new ArrayList<String>();
    private Hashtable<String, String> htmls = new Hashtable<String, String>();
    public String fileFolder = "";
    public String logFname = "";
    private PrintWriter pw = null;
    
	public void init_db_connection(String schema, String db) throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException
	{
		this.schema = schema;
		this.db = db;
				
	    Class.forName ("com.mysql.jdbc.Driver").newInstance ();
	    conn = DriverManager.getConnection (dburl, userName, password);          	
	    stmt = (Statement) conn.createStatement();
	    
	    System.out.println ("Connecting to " + dburl);
	    System.out.println ("Database connection established");
	}
	
	public void init_log() throws IOException
	{
		pw = new PrintWriter(new FileWriter(logFname));
	}
	
	public void close_log()
	{
		pw.flush();
		pw.close();
	}
	
	public void close_db() throws IOException, SQLException, InstantiationException, IllegalAccessException, ClassNotFoundException
	{			
  	   if(stmt != null)
       {
      	  try
            {            	  
                stmt.close ();
                System.out.println ("Database statement terminated");
            }
            catch (Exception e) { System.err.println(e); }
        }
        if (conn != null)
        {
            try
            {            	  
                conn.close ();
                System.out.println ("Database connection terminated");
            }
            catch (Exception e) {System.err.println(e); }
        }               
	}
	
	public void close() throws IOException, SQLException, InstantiationException, IllegalAccessException, ClassNotFoundException
	{	
		if(htmls.size() > 0)
		{
			//write data into db
			insert_data_to_db(htmls, docIDs);
			//clear cache
			htmls.clear();
	        docIDs.clear();
	        
	        System.out.println("Adding " + commitSize + " documents");
		}
		
  	   if(stmt != null)
       {
      	  try
            {            	  
                stmt.close ();
                System.out.println ("Database statement terminated");
            }
            catch (Exception e) { System.err.println(e); }
        }
        if (conn != null)
        {
            try
            {            	  
                conn.close ();
                System.out.println ("Database connection terminated");
            }
            catch (Exception e) {System.err.println(e); }
        }               
	}
	
	
	boolean insert_doc_to_db(String id, StringBuilder sb) throws SQLException, InstantiationException, IllegalAccessException, ClassNotFoundException
	{		
		try
		{
			String sql = "insert into "+schema+"."+db+"(id, html, text, features) values (?, ?, ?, ?)";
			
			PreparedStatement pStmt = conn.prepareStatement(sql);
			pStmt.setString(1, id);		
			pStmt.setString(2, sb.toString());
			pStmt.setString(3, "");
			pStmt.setString(4, "");						    		   
		    		
		    pStmt.executeUpdate();
		    System.out.println("insert " + id);
			return true;
		}
		catch(com.mysql.jdbc.exceptions.jdbc4.MySQLNonTransientConnectionException e)
		{
			if(stmt != null)
	       {
	      	  try
	            {            	  
	                stmt.close ();
	                System.out.println ("Database statement terminated");
	            }
	            catch (Exception e2) { System.err.println(e2); }
	        }
	        if (conn != null)
	        {
	            try
	            {            	  
	                conn.close ();
	                System.out.println ("Database connection terminated");
	            }
	            catch (Exception e3) {System.err.println(e3); }
	        }
	        
	        Class.forName ("com.mysql.jdbc.Driver").newInstance ();
		    conn = DriverManager.getConnection (dburl, userName, password);          	
		    stmt = (Statement) conn.createStatement();
		    
		    return false;
		}
		catch(com.mysql.jdbc.exceptions.jdbc4.MySQLIntegrityConstraintViolationException e)
		{
			System.out.println("insert " + id + " duplicate.");
			return true;
		}
		catch(Exception e)
		{
			System.out.println("insert " + id + " wrong.");
			e.printStackTrace();
			return false;
		}				
	}
	
	
	void extract_all_links(String html, ArrayList<String> all_links, ArrayList<String> all_link_text)
	{
		try
		{
			Document doc = Jsoup.parse(html);
			Elements links = doc.select("a[href]");
			for (Element link : links) {
				String url = link.attr("abs:href");
				String text = link.text();
				all_links.add(url);
				all_link_text.add(text);           
	        }
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}
	
	void save_db(String docID, ArrayList<String> all_links, ArrayList<String> all_link_text)
	{
		/*
		 try:
			num_out_links = len(all_links)
			out_link_str = '<<>>'.join(all_links)		
			out_text_str = '<<>>'.join(all_link_text)	
			sql = 'insert into '+ schema + '.' + tb + ' values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
			cursor.execute(sql, [did, '', '', '', '', 0, '', out_link_str, out_text_str, num_out_links, '', ''])
		except Exception:
			fw.write('update error ' + did + '\n')	
		 */
		int num_out_links = all_links.size();
		String out_link_str = join(all_links, "<<>>");
		String out_text_str = join(all_link_text, "<<>>");
		
		{
			try
			{
				String sql = "insert into "+schema+"."+text_tb+" (id,url,in_link_ids,in_link_urls,in_text_list,number_in_links,out_link_ids,out_link_urls,out_link_text,number_out_links,all_anchor_text,features) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
				
				PreparedStatement pStmt = conn.prepareStatement(sql);
				pStmt.setString(1, docID);		
				pStmt.setString(2, "");
				pStmt.setString(3, "");
				pStmt.setString(4, "");
				pStmt.setString(5, "");
				pStmt.setString(6, "0");
				pStmt.setString(7, "");
				pStmt.setString(8, out_link_str);
				pStmt.setString(9, out_text_str);
				pStmt.setString(10, ""+num_out_links);
				pStmt.setString(11, "");
				pStmt.setString(12, "");
			    		
			    pStmt.executeUpdate();
			    System.out.println("insert " + docID);
				
			}			
			catch(Exception e)
			{
				System.out.println("insert " + docID + " wrong.");
				e.printStackTrace();				
			}
		}
	}
	
	void insert_data_to_db(Hashtable<String, String> docs, ArrayList<String> docIDs) throws SQLException, InstantiationException, IllegalAccessException, ClassNotFoundException, IOException
	{
		init_db_connection(this.schema, this.text_tb);
		
		for(int i=0;i<docIDs.size();i++)
		{
			String docID = docIDs.get(i);
			String html = (String)docs.get(docID);
			
			ArrayList<String> all_links = new ArrayList<String>();
			ArrayList<String> all_link_texts = new ArrayList<String>();
			
			extract_all_links(html, all_links, all_link_texts);
			save_db(docID, all_links, all_link_texts);
			
			all_links.clear();
			all_link_texts.clear();
			
			//extract out link and text
			System.out.println("Insert DB " + docID + " successful");
		}
		
		close_db();
	}
	
	public void readDocIDList(ArrayList<String> docIDList, String fname) throws IOException
	{		
		String sCurrentLine;
		BufferedReader br = new BufferedReader(new FileReader(fname));	 
		while ((sCurrentLine = br.readLine()) != null) {
			docIDList.add("\""+sCurrentLine+"\"");
		}						
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
	
	public void query_html(List<String> subList) throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException, IOException
	{
		init_db_connection(this.schema, this.html_tb);
		
		String sql = "select id,html from "+this.schema+"."+this.html_tb+" where id in ("+join(subList, ",")+")";
		
		ResultSet rs = null;
		  try
		 {
			 rs = stmt.executeQuery(sql) ;	  
		 }
		 catch(Exception ex)
		 {
			 //close database connection
			System.out.println("Database Exception. Try to reset connection");
			 close();
			 //establish the link to the database again
			 init_db_connection(this.schema, this.db);	       
		 }
		  // Loop through the result set
		  while( rs.next() )
		  {			 			
			//get doc ID, this is a digit number read from DB
			String id = rs.getString(1);					
			String html = rs.getString(2);
			
			docIDs.add(id);
			htmls.put(id, html);
		  }
		 
		  rs.close() ;
		
		close_db();
	}
	
	public void checkCommit() throws IOException, SQLException, InstantiationException, IllegalAccessException, ClassNotFoundException
	{
		if(htmls.size() >= commitSize)
		{
			//write data into db
			insert_data_to_db(htmls, docIDs);
			//clear cache
			htmls.clear();
	        docIDs.clear();
	        
	        System.out.println("Adding " + commitSize + " documents");
		}
	}
	
	/**
	 * @param args
	 * @throws SQLException 
	 * @throws ClassNotFoundException 
	 * @throws IllegalAccessException 
	 * @throws InstantiationException 
	 * @throws IOException 
	 */
	public static void main(String[] args) throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException, IOException {
		// TODO Auto-generated method stub
		if (args.length != 10) {
		      System.err.println("Usage: DownloadCluewb09 [-schema <db_schema>] [-html_tb <html_tb>] [-text_tb <text_tb>] [-idfname <id_list_fname>] [-logFname <logFname>]");
		      return ;
		    }		  
		  String db_schema = "";
		  String db_tb = "";
		  String id_fname = "";		  
		  String logFname = "";
		  String html_tb = "";
		  String text_tb = "";
		// TODO Auto-generated method stub
		    for (int i = 0; i < args.length; i++) {
		    	if (args[i].equals("-schema")) {
		    	  db_schema = args[++i];
		      }else if (args[i].equals("-html_tb")) {
		    	  html_tb = args[++i];
		      }else if (args[i].equals("-text_tb")) {
		    	  text_tb = args[++i];
		      }	      
		      else if (args[i].equals("-idfname")) {
		    	  id_fname = args[++i];
		      }			      
		      else if (args[i].equals("-logFname")) {
		    	  logFname = args[++i];
		      }
		    }		
		
		ExtractAndSaveOutLinks p = new ExtractAndSaveOutLinks();
		//p.init_db_connection(db_schema, db_tb);
		p.schema = db_schema;
		p.db = db_tb;		
		p.logFname = logFname;
		p.text_tb = text_tb;
		p.html_tb = html_tb;
		
		ArrayList<String> docIDList = new ArrayList<String> ();		
		p.readDocIDList(docIDList, id_fname);		
		
		int nDocs = docIDList.size();
		int to = 0;		
		for(int i=0;i<nDocs;i+=p.querySize)
		{
			//sublist as fixed interval
			to = (i+p.querySize > nDocs)?nDocs:i+p.querySize; 
			List<String> subList = docIDList.subList(i, to);
			
			//query db to get docs
			p.query_html(subList);
			p.checkCommit();
		}		
		
		p.close();
		
	}

}
