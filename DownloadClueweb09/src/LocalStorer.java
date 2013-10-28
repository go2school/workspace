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
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Hashtable;
import java.util.List;
import java.util.Set;

import com.mysql.jdbc.Statement;

public class LocalStorer {

	private Connection conn = null;	
	private Statement stmt = null ;
	public String schema = "";
	public String db = "";
	private String dburl = "jdbc:mysql://192.168.0.2:3306/";
	private String userName = "root";
	private String password = "see";
	private int commitWithin = 300000;//5 mins
    public int querySize = 50;
    private int commitSize = 50;
    private Hashtable<String, StringBuilder> docs = new Hashtable<String, StringBuilder>();
    private ArrayList<String> docIDs = new ArrayList<String>();
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
		if(docs.size() > 0)
		{
			//write data into db
			insert_data_to_db(docs, docIDs);
			//clear cache
	        docs.clear();
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
	
	public StringBuilder download_webpage(String url)
	{
	  HttpURLConnection connection = null;
	  OutputStreamWriter wr = null;
	  BufferedReader rd  = null;
	  StringBuilder sb = null;
	  String line = null;
	
	  URL serverAddress = null;
	
	  try {
	      serverAddress = new URL(url);
		  //set up out communications stuff
		  connection = null;
				
		  //Set up the initial connection
		  connection = (HttpURLConnection)serverAddress.openConnection();
		  connection.setRequestMethod("GET");
		  connection.setRequestProperty("Authorization", "Basic dXdvOlRoYW1lc1JpdmVy");
		  connection.setRequestProperty("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8");
		  connection.setRequestProperty("Accept-Charset", "ISO-8859-1,utf-8;q=0.7,*;q=0.3");
		  connection.setRequestProperty("Accept", "gzip,deflate,sdch");
		  connection.setRequestProperty("Accept-Language", "en-US,en;q=0.8");
		  connection.setRequestProperty("Authorization", "Basic dXdvOlRoYW1lc1JpdmVy");
		  connection.setRequestProperty("Cache-Control", "max-age=0");
		  connection.setRequestProperty("Connection", "keep-alive");
		  connection.setRequestProperty("Cookie", "PHPSESSID=ieq98nd9av0h5mia0jf11; local_tz=%28EST%29");
		  connection.setRequestProperty("Host", "boston.lti.cs.cmu.edu");
		  connection.setRequestProperty("User-Agent", "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.97 Safari/537.11");
		  connection.setDoOutput(true);
		  connection.setReadTimeout(10000);
		            
		  connection.connect();				
		
		  //read the result from the server
		  rd  = new BufferedReader(new InputStreamReader(connection.getInputStream()));
		  sb = new StringBuilder();
		
		  while ((line = rd.readLine()) != null)
		  {
		      sb.append(line + '\n');
		  }		    		  
		                
	  } catch (MalformedURLException e) {		 
	      e.printStackTrace();
	  } catch (ProtocolException e) {
	      e.printStackTrace();
	  } catch (IOException e) {
	      e.printStackTrace();
	  }
	  finally
	  {
	      //close the connection, set all objects to null
	      connection.disconnect();
	      rd = null;	      
	      wr = null;
	      connection = null;
	  }
	  
	  return sb;
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
	
	boolean write_doc_to_folder(String folder, String id, StringBuilder sb)
	{
		try
		{
			PrintWriter pw = new PrintWriter(new FileWriter(folder + "/" + id));
			pw.print(sb.toString());
			pw.flush();
			pw.close();			
			System.out.println("DB Error write " + id + " to file.");
			return true;
		}
		catch(Exception e)
		{
			System.out.println("write " + id + " wrong.");
			pw.println("write " + id + " wrong");
			return false;
		}
	}
	
	void insert_data_to_db(Hashtable<String, StringBuilder> docs, ArrayList<String> docIDs) throws SQLException, InstantiationException, IllegalAccessException, ClassNotFoundException, IOException
	{
		init_db_connection(this.schema, this.db);
		
		for(int i=0;i<docIDs.size();i++)
		{
			String docID = docIDs.get(i);
			StringBuilder sb = (StringBuilder)docs.get(docID);
			boolean insertFlag = insert_doc_to_db(docID, sb);
			if(insertFlag == false)
				write_doc_to_folder(fileFolder, docID, sb);
			else
				System.out.println("Insert DB " + docID + " successful");
		}
		
		close_db();
	}
	
	public void readDocIDList(ArrayList<String> docIDList, String fname) throws IOException
	{		
		String sCurrentLine;
		BufferedReader br = new BufferedReader(new FileReader(fname));	 
		while ((sCurrentLine = br.readLine()) != null) {
			docIDList.add(sCurrentLine);
		}						
	}
	
	public void do_download(List<String> subList, String url) throws SQLException
	{
		 for(int i=0;i<subList.size();i++)
		 {
			 String docID = subList.get(i);
			 String to_url = url + docID;
			 StringBuilder sb = download_webpage(to_url);
			 if(sb != null)
			 {
				 docs.put(docID, sb);
				 docIDs.add(docID);
				 
				 System.out.println("Download " + docID + " successful");
			 }
		 }
	}
	
	public boolean read_data(String id_fname, StringBuilder sb) throws IOException
	{		
		try
		{
			BufferedReader br = new BufferedReader(new FileReader(id_fname));
			String line = null;
			while((line = br.readLine()) != null)
				sb.append(line + "\n");
			br.close();
			return true;
		}
		catch(Exception e)
		{
			e.printStackTrace();
			return false;
		}
	}
	
	public void do_process(String folder, List<String> subList, String url) throws SQLException, IOException
	{
		 for(int i=0;i<subList.size();i++)
		 {
			 String docID = subList.get(i);
			 String to_url = url + docID;
			 StringBuilder sb = new StringBuilder(); 
			boolean flag = read_data(folder + "/" + docID, sb); 
			if(flag == false)		 
					sb = download_webpage(to_url);
			 if(sb != null)
			 {
				 docs.put(docID, sb);
				 docIDs.add(docID);
				 
				 System.out.println("Download " + docID + " successful");
			 }
		 }
	}
	
	public void checkCommit() throws IOException, SQLException, InstantiationException, IllegalAccessException, ClassNotFoundException
	{
		if(docs.size() >= commitSize)
		{
			//write data into db
			insert_data_to_db(docs, docIDs);
			//clear cache
	        docs.clear();
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
		      System.err.println("Usage: DownloadCluewb09 [-schema <db_schema>] [-tb <tb>] [-idfname <id_list_fname>] [-fileFolder <fileFolder>] [-logFname <logFname>]");
		      return ;
		    }		  
		  String db_schema = "";
		  String db_tb = "";
		  String id_fname = "";
		  String arg_folder = "";
		  String logFname = "";
		  
		// TODO Auto-generated method stub
		    for (int i = 0; i < args.length; i++) {
		    	if (args[i].equals("-schema")) {
		    	  db_schema = args[++i];
		      }else if (args[i].equals("-tb")) {
		    	  db_tb = args[++i];
		      }	      
		      else if (args[i].equals("-idfname")) {
		    	  id_fname = args[++i];
		      }	
		      else if (args[i].equals("-fileFolder")) {
		    	  arg_folder = args[++i];
		      }	
		      else if (args[i].equals("-logFname")) {
		    	  logFname = args[++i];
		      }
		    }		
		
		LocalStorer p = new LocalStorer();
		//p.init_db_connection(db_schema, db_tb);
		p.schema = db_schema;
		p.db = db_tb;
		p.fileFolder = arg_folder;
		p.logFname = logFname;
		
		String clueweb09_url = "http://boston.lti.cs.cmu.edu/Services/search/clueweb09_cata_english/lemur.cgi?e=";				
		
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
			p.do_process(arg_folder, subList, clueweb09_url);
			p.checkCommit();
		}		
		
		p.close();
		
	}

}
