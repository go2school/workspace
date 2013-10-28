import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;


public class DataInsertor {
	
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
    
    public void insertData(String fname1, String fname2)
    {
    	
    }
    
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		if (args.length != 18) {
		      System.err.println("Usage: DataInsertor [-schema <db_schema>] [-tb <table_name>] [-fields <field,field,field>] ");
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
		DataInsertor db = new DataInsertor();		
		db.init(db_schema);			
		db.queryDBGetIDAndFields(outid, sep, id_fname, key, keyType, fields, db_table, ofname);
		db.close();
	}

}
