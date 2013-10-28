import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Hashtable;


public class WikiToDB {
	
	public String inputFname = "/home/xiao/top300000.txt";
	
	public String userName = "root";
	public String password = "see";
	public String doc_db = "enwikipedia.pages";
	public String dburl = "jdbc:mysql://192.168.0.2:3306/enwikipedia?characterEncoding=UTF-8";
    
	public Connection conn = null;
	public PreparedStatement stmt = null;		
	
	public void init_db_connection() throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException
	{
		Class.forName ("com.mysql.jdbc.Driver").newInstance();
        conn = DriverManager.getConnection (dburl, userName, password);
        stmt = (PreparedStatement) conn.prepareStatement("INSERT INTO "+doc_db+" (id, timestamp, orglength, newlength, stub, disambig, category, image, title, categories, links, related, text) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
	}
	
	public void close_db_connection()
	{
		if(stmt != null)
        {
      	  try
            {            	  
                stmt.close ();
                System.out.println ("Database connection terminated");
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
	
	public boolean parse_wikipedia_dump(BufferedReader br, Hashtable<String, String> doc_data) throws IOException
	{		
		boolean begin_text = false;
		String text = "";
		boolean end_doc = false;
		while(true)
		{		
			String line = br.readLine();						
			if (line == null)
				break;
			else if(line.startsWith("<page id="))
			{
				//<page id="39" timestamp="2011-12-18T21:03:24Z" orglength="24533" newlength="18805" stub="0" disambig="0" category="0" image="0">
				int start_pos = line.indexOf(" ") + 1;
				int end_pos = line.lastIndexOf(">");
				String [] tokens = line.substring(start_pos, end_pos).split(" ");
				for(int i=0;i<tokens.length;i++)
				{
					String [] tmp = tokens[i].split("=");
					doc_data.put(tmp[0], tmp[1].replace("\"", ""));					
				}
			}
			else if (line.startsWith("<title>"))
			{
				int start_pos = line.indexOf(">") + 1;
				int end_pos = line.lastIndexOf("<");
				doc_data.put("title", line.substring(start_pos, end_pos));				
			}
			else if (line.startsWith("<categories>"))
			{
				int start_pos = line.indexOf(">") + 1;
				int end_pos = line.lastIndexOf("<");
				doc_data.put("categories", line.substring(start_pos, end_pos));				
			}
			else if (line.startsWith("<links>"))
			{
				int start_pos = line.indexOf(">") + 1;
				int end_pos = line.lastIndexOf("<");
				doc_data.put("links", line.substring(start_pos, end_pos));				
			}
			else if (line.startsWith("<related>"))
			{
				int start_pos = line.indexOf(">") + 1;
				int end_pos = line.lastIndexOf("<");
				doc_data.put("related", line.substring(start_pos, end_pos));				
			}	
			else if (line.startsWith("</templates><text>"))
			{
				begin_text = true;		
			}
			else if (line.startsWith("</text>"))
			{
				begin_text = false;		
			}
			else if (line.startsWith("</page>"))
			{
				end_doc = true;		
				break;
			}					
			else if (begin_text == true)
				text += line;
		}
		doc_data.put("text", text);
		return end_doc;
	}
	
	public void commitDB()
	{
		try
        {            	  
            conn.commit();            
        }
        catch (Exception e) { System.err.println(e); }
	}
	
	public void insertDB(Hashtable<String, String> doc) throws SQLException
	{			
		try
		{
			stmt.clearParameters();
			stmt.setString(1, (String) doc.get("id"));
			stmt.setString(2, (String) doc.get("timestamp"));
			stmt.setString(3, (String) doc.get("orglength"));
			stmt.setString(4, (String) doc.get("newlength"));
			stmt.setString(5, (String) doc.get("stub"));
			stmt.setString(6, (String) doc.get("disambig"));
			stmt.setString(7, (String) doc.get("category"));
			stmt.setString(8, (String) doc.get("image"));
			stmt.setString(9, (String) doc.get("title"));
			stmt.setString(10, (String) doc.get("categories"));
			stmt.setString(11, (String) doc.get("links"));
			stmt.setString(12, (String) doc.get("related"));
			stmt.setString(13, (String) doc.get("text"));
			stmt.execute();
			System.out.println("insert " + doc.get("id") + " successfully");			
		}
		catch(Exception e)
		{
			System.err.println("insert " + doc.get("id") + " failed");
			System.err.println(e);
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
		if(args.length != 1)
		{
			System.out.println("Usage <program> <XML dump file>");
			System.exit(0);
		}
		WikiToDB w = new WikiToDB();
		w.inputFname = args[0];
		w.init_db_connection();
		BufferedReader br = new BufferedReader(new FileReader(w.inputFname));
		Hashtable<String, String> doc = new Hashtable<String, String>();
		
		while(true)
		{			
			doc.clear();
			boolean ret = w.parse_wikipedia_dump(br, doc);
			if (ret == false)
				break;
			else
			{
				w.insertDB(doc);
			}		
		}	
		br.close();
		w.close_db_connection();		
	}

}
