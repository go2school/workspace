import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.StringEscapeUtils;
import net.htmlparser.jericho.*;

public class ExtractRank{

	class Doc
	{
		public String title = "";
		public String desc = "";
		public String url = "";
		public String content = "";
		public String path = "";
		public int rank = -1;
	}
	
	public String base_folder = "/home/xiao/wiki_subject_hierarchy/SEEUWO_Training_Data";
	public String dbtable = "webdoc_ranks";
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
        stmt = (PreparedStatement) conn.prepareStatement("INSERT INTO "+dbtable+" (url, path, rank) values (?, ?, ?)");
        
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
    
	
	public String uncoverHTML(String str)
	{
		String ret = StringUtils.strip(str);
		ret = ret.substring(9, ret.length()- 3);		
		String ret1 = ret.replaceAll("&#\\d+", "");
		ret1 = StringEscapeUtils.unescapeHtml3(ret1);
		
		return ret1;
	}
	
	public ArrayList<Doc> new_parse_xml_url_rank(String prefix, String fname) throws IOException
	{
		ArrayList<Doc> docList = new ArrayList<Doc>();
		BufferedReader br = new BufferedReader(new FileReader(fname));
		String buffer = "";
		Doc curDoc = null;
		int n = 0;
		while((buffer = br.readLine()) != null)
		{
			if(buffer.startsWith("</WEB:WEBRESULT>"))
			{
				docList.add(curDoc);
			}
			else if(buffer.startsWith("<WEB:TITLE>"))
			{
				Doc d = new Doc();				
				curDoc = d;
				curDoc.path =  fname;
				curDoc.rank = n;
				n += 1;
				buffer = br.readLine();			
				buffer = br.readLine();				
			}
			else if(buffer.startsWith("<WEB:URL>"))
			{
				buffer = br.readLine();
				curDoc.url = uncoverHTML(buffer);
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
				stmt.setString(2, d.path);
				stmt.setInt(3, d.rank);									
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
		String [] argv = new String[2];
		argv[0] = "/home/xiao/workspace/Extractor/loca_xml.txt";
		argv[1] = "all_xml.txt";
		//read xml file list
		BufferedReader br = new BufferedReader(new FileReader(argv[0]));
		String prefix = argv[1];
		//BufferedReader br = new BufferedReader(new FileReader(argv[0]));
		String tmp = null;
		ArrayList<String> fnames = new ArrayList<String>();
		while((tmp = br.readLine()) != null)
		{
			fnames.add(StringUtils.strip(tmp));
		}
		br.close();
		
		System.out.println("XXX" + prefix);
		//start extract rank
		ExtractRank e = new ExtractRank();
		e.init();		
		for(int i=0;i<fnames.size();i++)
		{
			ArrayList<Doc> docs = e.new_parse_xml_url_rank(prefix, fnames.get(i));
			e.insertDB(docs);
		}
		e.close();
	}
}
