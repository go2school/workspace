import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import com.mysql.jdbc.Statement;

public class DumpFieldsByIDTool {
	
	public String dbtable = "webdoc";
	public String logFname = "proces.log";
	PrintWriter pwlog = null;
	private String userName = "root";
	private String password = "see";	
	private String dburl = "jdbc:mysql://192.168.0.2:3306/query_search_engine?characterEncoding=UTF-8";          
    private PreparedStatement stmt = null;    
    private Connection conn = null;
    
    public void init(String schema) throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException, IOException
    {
    	if(schema.equalsIgnoreCase(""))
    	{
    		System.out.println("schema name is missing");
    		System.exit(1);
    	}
    	dburl = "jdbc:mysql://192.168.0.2:3306/"+schema+"?characterEncoding=UTF-8";
    	Class.forName ("com.mysql.jdbc.Driver").newInstance ();
        conn = DriverManager.getConnection (dburl, userName, password);          
        //stmt = (PreparedStatement) conn.prepareStatement("INSERT INTO "+dbtable+" (url, title, description, html, text, path) values (?, ?,  ?, ?, ? ,?)");       
        pwlog = new PrintWriter(new FileWriter(logFname));               
    }
    
    public void close()
    {
    	if(stmt != null)
        {
      	  try
            {            	  
                stmt.close ();
                pwlog.println ("Database connection terminated");
            }
            catch (Exception e) { System.err.println(e); }
        }
        if (conn != null)
        {
            try
            {                        	  
                conn.close ();
                pwlog.println ("Database connection terminated");
            }
            catch (Exception e) {System.err.println(e); }
        }
        pwlog.flush();
        pwlog.close();
    }
 
    public void queryDBGetIDs(String outputFname) throws SQLException, IOException
    {    
    	String sql = "select id from query_search_engine.webdoc order by id asc";
    	//
    	PrintWriter pw = new PrintWriter(new FileWriter(outputFname));
    	Statement st = null;
    	ResultSet rs = null;
    	try
	    {
	      st = (Statement) conn.createStatement();	      
	      rs = st.executeQuery(sql);
	      while (rs.next())
	      {
	    	  String id = rs.getString(1);	    	  
	        
	        pw.println(id);
	      }
	    }
	    catch (SQLException ex)
	    {
	      System.err.println(ex.getMessage());	      
	    } 
    	finally
    	{
    		st.close();
    		rs.close();
    	}
    	pw.flush();
    	pw.close();
    }
    
    public void queryDBGetIDList(String idField, String schema, String table, String outputFname) throws SQLException, IOException
    {    
    	String sql = "select "+idField+" from "+schema+"."+table+" order by "+idField+" asc";
    	//
    	PrintWriter pw = new PrintWriter(new FileWriter(outputFname));
    	Statement st = null;
    	ResultSet rs = null;
    	try
	    {
	      st = (Statement) conn.createStatement();	      
	      rs = st.executeQuery(sql);
	      while (rs.next())
	      {
	    	  String id = rs.getString(1);	    	  
	        
	        pw.println(id);
	      }
	    }
	    catch (SQLException ex)
	    {
	      System.err.println(ex.getMessage());	      
	    } 
    	finally
    	{
    		st.close();
    		rs.close();
    	}
    	pw.flush();
    	pw.close();
    }
    
    public void queryDBGetIDAndURL(String idFname) throws SQLException, IOException
    {   
    	//read all ids
    	ArrayList<String> idList = new ArrayList<String>();
    	BufferedReader br = new BufferedReader(new FileReader(idFname));
    	String line = null;
    	while((line = br.readLine()) != null)
    	{
    		idList.add("" + Integer.parseInt(line));
    	}
    	br.close();
    	
    	//String outputFname = "query_bing_id_url_list.txt";
    	String outputFname = "/media/01CC16F5ED7072F0/seeuwo/url_hierarchical_clustering/new_3_uwo_all_solr_doc_id_url_list.txt";
    	PrintWriter pw = new PrintWriter(new FileWriter(outputFname));
    	
    	//query db to get id and urls
    	Statement st = null;
		st = (Statement) conn.createStatement();
		int trunk_size = 100;
		int nDocs = idList.size();
		for(int i=0;i<nDocs;i+=trunk_size)
		{
			//build a small trunk
			int to = (i+trunk_size > nDocs)?nDocs:i+trunk_size; 
			List<String> subList = idList.subList(i, to);			
    							
			//build the query string
			//String sql = "select id ,url from query_search_engine.webdoc where id in (";
			String sql = "select id ,url from uwo.uwo_new_nutch_docs where id in (";
    		for(int j=0;j<subList.size()-1;j++)
    			sql += subList.get(j) + ",";
    		sql += subList.get(subList.size()-1) + ")";
    		
    		//query db
        	ResultSet rs = null;
        	try
    	    {    	      	      
    	      rs = st.executeQuery(sql);
    	      while (rs.next())
    	      {
    	    	  String id = rs.getString(1);
    	    	  String path = rs.getString(2);
    	        
    	        pw.println(id + " " + path);
    	      }
    	    }
    	    catch (SQLException ex)
    	    {
    	      System.err.println(ex.getMessage());	      
    	    } 
        	finally
        	{        		
        		rs.close();
        	}
    	}    	
		st.close();
    	pw.flush();
    	pw.close();
    }
    
   
    public void queryDBGetIDAndField(String idFname, String filedName, String dbName, String outputFname) throws SQLException, IOException
    {   
    	//read all ids
    	ArrayList<String> idList = new ArrayList<String>();
    	BufferedReader br = new BufferedReader(new FileReader(idFname));
    	String line = null;
    	while((line = br.readLine()) != null)
    	{
    		idList.add("" + Integer.parseInt(line));
    	}
    	br.close();
    	
    	//String outputFname = "query_bing_id_url_list.txt";    	
    	PrintWriter pw = new PrintWriter(new FileWriter(outputFname));
    	
    	//query db to get id and urls
    	Statement st = null;
		st = (Statement) conn.createStatement();
		int trunk_size = 100;
		int nDocs = idList.size();
		for(int i=0;i<nDocs;i+=trunk_size)
		{
			//build a small trunk
			int to = (i+trunk_size > nDocs)?nDocs:i+trunk_size; 
			List<String> subList = idList.subList(i, to);			
    							
			//build the query string
			//String sql = "select id ,url from query_search_engine.webdoc where id in (";
			String sql = "select id ,"+filedName+" from "+dbName+" where id in (";
    		for(int j=0;j<subList.size()-1;j++)
    			sql += subList.get(j) + ",";
    		sql += subList.get(subList.size()-1) + ")";
    		
    		//query db
        	ResultSet rs = null;
        	try
    	    {    	      	      
    	      rs = st.executeQuery(sql);
    	      while (rs.next())
    	      {
    	    	  String id = rs.getString(1);
    	    	  String path = rs.getString(2);
    	        
    	        pw.println(id + " " + path);
    	      }
    	    }
    	    catch (SQLException ex)
    	    {
    	      System.err.println(ex.getMessage());	      
    	    } 
        	finally
        	{        		
        		rs.close();
        	}
    	}    	
		st.close();
    	pw.flush();
    	pw.close();
    }
    
    public void queryDBGetIDAndFields(String outid, String sep, String idFname, String idField, String keytype, String fields, String dbName, String outputFname) throws SQLException, IOException
    {   
    	//read all ids
    	ArrayList<String> idList = new ArrayList<String>();
    	BufferedReader br = new BufferedReader(new FileReader(idFname));
    	String line = null;
    	while((line = br.readLine()) != null)
    	{
    		idList.add(line);
    	}
    	br.close();
    	
    	PrintWriter pw = new PrintWriter(new FileWriter(outputFname));    	    	
    	
    	//query db to get id and urls
    	Statement st = null;
		st = (Statement) conn.createStatement();
		int trunk_size = 100;
		int nDocs = idList.size();
		String [] fs = fields.split(",");
		int n_field = fs.length;
		for(int i=0;i<nDocs;i+=trunk_size)
		{
			//build a small trunk
			int to = (i+trunk_size > nDocs)?nDocs:i+trunk_size; 
			List<String> subList = idList.subList(i, to);			
    							
			//build the query string			
			String sql = "select "+idField+" ,"+fields+" from "+dbName+" where "+idField+" in (";
    		for(int j=0;j<subList.size()-1;j++)
    		{
    			if(keytype.equals("int"))    			
    				sql += subList.get(j) + ",";
    			else if(keytype.equals("string"))    			
    				sql += "\"" + subList.get(j) + "\",";
    		}
    		if(keytype.equals("int")) 
    			sql += subList.get(subList.size()-1) + ")";
    		else if(keytype.equals("string")) 
    			sql += "\"" + subList.get(subList.size()-1) + "\")";
    		
    		//query db
        	ResultSet rs = null;
        	try
    	    {    	      	      
    	      rs = st.executeQuery(sql);
    	      
    	      while (rs.next())
    	      {
    	    	  String output = "";    	    	  
    	    	  String id = rs.getString(1);
    	    	  if(outid.equals("y"))
    	    		 output += id;
    	    	  for(int j=0;j<n_field;j++)
    	    	  {
    	    		  if(j == 0 && outid.equals("n"))
    	    			  output += rs.getString(j+2);
    	    		  else
    	    			  output += sep + rs.getString(j+2);
    	    	  }
    	        pw.println(output);    	        
    	      }
    	    }
    	    catch (SQLException ex)
    	    {
    	      System.err.println(ex.getMessage());	      
    	    } 
        	finally
        	{        		
        		rs.close();
        	}
    	}    	
		st.close();
    	pw.flush();
    	pw.close();
    }
    
    public String queryDBGetField(String id, String filedName, String dbName) throws SQLException, IOException
    {   
    	//query db to get id and urls
    	Statement st = null;
		st = (Statement) conn.createStatement();
		String ret = "";
			
		{			    			
			String sql = "select "+filedName+" from "+dbName+" where id =" + id;
    		
    		//query db
        	ResultSet rs = null;
        	try
    	    {    	      	      
    	      rs = st.executeQuery(sql);
    	      while (rs.next())
    	      {
    	    	  ret = rs.getString(1);
    	    	  break;
    	      }
    	    }
    	    catch (SQLException ex)
    	    {
    	      System.err.println(ex.getMessage());	      
    	    } 
        	finally
        	{        		
        		rs.close();
        	}
    	}    	
		st.close();
    	return ret;
    }
    
    public void getEmptyDoc(String dbName, String output) throws SQLException, IOException
    {
    	//String sql = "select id,url,path from "+dbName+" where text = '' order by id asc";
    	String sql = "select id from "+dbName+" where text = '' order by id asc";
    	Statement st = null;
		st = (Statement) conn.createStatement();		
		//query db
		PrintWriter pw = new PrintWriter(new FileWriter(output));
    	ResultSet rs = null;
    	try
	    {    	      	      
	      rs = st.executeQuery(sql);
	      
	      while (rs.next())
	      {	    	  
	    	  String id = rs.getString(1);
	    	  String url = rs.getString(2);
	    	  String path = rs.getString(3);
	       // pw.println(id + "|" + url + "|" + path);
	    	  pw.println(id );
	        
	      }
	    }
	    catch (SQLException ex)
	    {
	      System.err.println(ex.getMessage());	      
	    } 
    	finally
    	{        		
    		rs.close();
    	}
    	pw.flush();
    	pw.close();
    }
    
    public void delete_by_ids(String fname, String dbName) throws IOException, SQLException
    {
    	BufferedReader br = new BufferedReader(new FileReader(fname));
    	ArrayList<String> ids = new ArrayList<String>();
    	String tmp = "";
    	while((tmp = br.readLine()) != null)
    	{
    		ids.add(tmp);
    	}
    	br.close();
    	Statement st = null;
		st = (Statement) conn.createStatement();
    	for(int i=0;i<ids.size();i++)
    	{
	    	String sql = "delete from "+dbName+" where id = " + ids.get(i);	    							    
		    st.execute(sql);
	    }    	
    }
	/**
	 * @param args
	 * @throws IOException 
	 * @throws SQLException 
	 * @throws ClassNotFoundException 
	 * @throws IllegalAccessException 
	 * @throws InstantiationException 
	 */
	public static void main(String[] args) throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException, IOException {
		
		 if (args.length != 18) {
		      System.err.println("Usage: DumpFieldsByIDTool [-outid <y|n>] [-sep sep] [-keytype <int|string>] [-schema <db_schema>] [-tb <table_name>] [-key <key>] [-fields <field,field,field>] [-idfname <id_fname>][-ofname <fname>]");
		      return ;
		    }
		  String db_table = "";		  
		  String db_schema = "";
		  String id_fname = "";
		  String ofname = "";
		  String fields = "";
		  String key = "";
		  String sep = "";	
		  String keyType = "";
		  String outid = "";//default put id at the begining
		// TODO Auto-generated method stub
		    for (int i = 0; i < args.length; i++) {
		    	if (args[i].equals("-tb")) {
		    		db_table = args[++i];
		      }else if (args[i].equals("-schema")) {
		    	  db_schema = args[++i];
		      }else if (args[i].equals("-fields")) {
		    	  fields = args[++i];
		      }
		      else if (args[i].equals("-ofname")) {
		    	  ofname = args[++i];
		      }
		      else if (args[i].equals("-idfname")) {
		    	  id_fname = args[++i];
		      }
		      else if (args[i].equals("-key")) {
		    	  key = args[++i];
		      }
		      else if (args[i].equals("-keytype")) {
		    	  keyType = args[++i];
		      }
		      else if (args[i].equals("-outid")) {
		    	  outid = args[++i];
		      }
		      else if (args[i].equals("-sep")) {
		    	  sep = args[++i];
		      }
		    }	
		    
		// TODO Auto-generated method stub
		DumpFieldsByIDTool db = new DumpFieldsByIDTool();		
		db.init(db_schema);			
		db.queryDBGetIDAndFields(outid, sep, id_fname, key, keyType, fields, db_table, ofname);
		db.close();
	}

}
