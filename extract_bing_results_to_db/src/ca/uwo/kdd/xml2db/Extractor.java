package ca.uwo.kdd.xml2db;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.StringEscapeUtils;
import org.jsoup.Jsoup;

public class Extractor {

	public class Doc
	{
		public String title = "";
		public String desc = "";
		public String url = "";
		public String content = "";
		public String path = "";
		
	}
	
	public String base_folder = "/media/9AFA0365FA033D4F/SEEUWO_Training_Data";
	public String dbtable = "webdoc";
	public String logFname = "proces.log";
	PrintWriter pwlog = null;
	private String userName = "root";
	private String password = "see";
	private String dburl = "jdbc:mysql://192.168.0.2:3306/query_search_engine?characterEncoding=UTF-8";          
    private PreparedStatement stmt = null;    
    private Connection conn = null;
    
    public void init() throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException, IOException
    {
    	Class.forName ("com.mysql.jdbc.Driver").newInstance ();
        conn = DriverManager.getConnection (dburl, userName, password);          
        stmt = (PreparedStatement) conn.prepareStatement("INSERT INTO "+dbtable+" (url, title, description, html, text, path) values (?, ?,  ?, ?, ? ,?)");        
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
    
	public void Process(File aFile) throws IOException, SQLException{	    
	    if(aFile.isFile())
	    {
	    	String path = aFile.getPath();	    	
	      System.out.println("[FILE] " + path);	 
	      
	      ArrayList<Doc> docList = parse_xml(path);
	      insertDB(docList);	      
	    }
	    else if (aFile.isDirectory()) {
	    	
	    		String path = aFile.getPath();	    		
		      System.out.println("[DIR] " + path);
	      File[] listOfFiles = aFile.listFiles();
	      if(listOfFiles!=null) {
	        for (int i = 0; i < listOfFiles.length; i++)
	          Process(listOfFiles[i]);
	      } else {
	        System.out.println(" [ACCESS DENIED]");
	      }	 
	      
	    }
	  }

	
	public String uncoverHTML(String str)
	{
		String ret = StringUtils.strip(str);
		ret = ret.substring(9, ret.length()- 3);		
		String ret1 = ret.replaceAll("&#\\d+", "");
		ret1 = StringEscapeUtils.unescapeHtml3(ret1);
		
		return ret1;
	}
	
	public ArrayList<Doc> parse_xml(String fname) throws IOException
	{
		ArrayList<Doc> docList = new ArrayList<Doc>();
		BufferedReader br = new BufferedReader(new FileReader(fname));
		String buffer = "";
		Doc curDoc = null;
		while((buffer = br.readLine()) != null)
		{
			if(buffer.startsWith("</WEB:WEBRESULT>"))
			{
				//do not skip empty document
				//if(curDoc.content != "")
				docList.add(curDoc);
			}
			else if(buffer.startsWith("<WEB:TITLE>"))
			{
				Doc d = new Doc();				
				curDoc = d;
				curDoc.path = fname;
				buffer = br.readLine();
				curDoc.title = uncoverHTML(buffer);
				buffer = br.readLine();				
			}
			else if(buffer.startsWith("<WEB:DESCRIPTION>"))
			{
				buffer = br.readLine();
				curDoc.desc = uncoverHTML(buffer);
				buffer = br.readLine();				
			}
			else if(buffer.startsWith("<WEB:URL>"))
			{
				buffer = br.readLine();
				curDoc.url = uncoverHTML(buffer);
				buffer = br.readLine();				
			}
			else if(buffer.startsWith("<WEB:CONTENT>"))
			{
				buffer = br.readLine();
				curDoc.content = uncoverHTML(buffer);
				buffer = br.readLine();
			}
		}
		br.close();
		return docList;
	}
	
	public void insertDB(ArrayList<Doc> docList) throws SQLException
	{			
		for(int i=0;i<docList.size();i++)
		{
			Doc d = docList.get(i);
			try
			{	
				
				stmt.setEscapeProcessing(true);
				stmt.clearParameters();
				
				stmt.setString(1, d.url);
				stmt.setString(2, d.title);
				stmt.setString(3, d.desc);
				stmt.setString(4, d.content);
				stmt.setString(5, Jsoup.parse(d.content).text());
				stmt.setString(6, d.path);		
				stmt.execute();					
			}
			catch(Exception e)
			{
				System.out.println(d.url + " " + e);				
				pwlog.println (d.url + " " + e);
			}
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
	public static void main(String[] args) throws IOException, InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException {
		// TODO Auto-generated method stub		
		Extractor e = new Extractor();
		e.init();
		e.Process(new File(e.base_folder));
		e.close();
	}

}
