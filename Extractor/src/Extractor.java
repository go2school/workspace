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
import java.util.TreeSet;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.StringEscapeUtils;
import org.jsoup.Jsoup;

import net.htmlparser.jericho.*;

public class Extractor {

	class WebDoc
	{
		public String title = "";
		public String desc = "";
		public String url = "";
		public String content = "";
		public String path = "";
		public int rank = -1;
	}
	
	public String base_folder = "/home/xiao/wiki_subject_hierarchy/SEEUWO_Training_Data";
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
	      
	      ArrayList<WebDoc> docList = parse_xml(path);
	      insertDB(docList);	      
	    }
	    else if (aFile.isDirectory()) {
	    	
	    	//added by xiao 03032012	    	
	    	
	    		String path = aFile.getPath();	    		
		      System.out.println("[DIR] " + path);
		      //we skip the jobs that are not professiona and social science		      
		      File[] listOfFiles = aFile.listFiles();
		      if(listOfFiles!=null) {
		        for (int i = 0; i < listOfFiles.length; i++)
		          Process(listOfFiles[i]);
		      } else {
		        System.out.println(" [ACCESS DENIED]");
		      }	 	      	      
	    }
	  }

	public void getAllFiles(File aFile, ArrayList<String> fnames) throws IOException, SQLException{	    
	    if(aFile.isFile())
	    {
	    	String path = aFile.getPath();	    	
	      System.out.println("[FILE] " + path);	 	      	     
	      fnames.add(path);      
	    }
	    else if (aFile.isDirectory()) {	    
	    		String path = aFile.getPath();	    		
		      System.out.println("[DIR] " + path);
		      //we skip the jobs that are not professiona and social science		      
		      File[] listOfFiles = aFile.listFiles();
		      if(listOfFiles!=null) {
		        for (int i = 0; i < listOfFiles.length; i++)
		        	getAllFiles(listOfFiles[i], fnames);
		      } else {
		        System.out.println(" [ACCESS DENIED]");
		      }	 	      	      
	    }
	  }
	
	public void getAllFiles(File aFile, ArrayList<String> fnames, int cur_depth, int depth_threshold) throws IOException, SQLException{	    
	    if(aFile.isFile())
	    {
	    	if(cur_depth >= depth_threshold)
	    	{
		    	String path = aFile.getPath();	    	
		      System.out.println("[FILE] " + path);	 	      	     
		      fnames.add(path);
	    	}
	    }
	    else if (aFile.isDirectory()) {	    
	    		String path = aFile.getPath();	    		
		      System.out.println("[DIR] " + path);
		      //we skip the jobs that are not professiona and social science		      
		      File[] listOfFiles = aFile.listFiles();
		      if(listOfFiles!=null) {
		        for (int i = 0; i < listOfFiles.length; i++)
		        	getAllFiles(listOfFiles[i], fnames, cur_depth +1, depth_threshold);
		      } else {
		        System.out.println(" [ACCESS DENIED]");
		      }	 	      	      
	    }
	  }
	
	public static void getAllFilePaths(File aFile, PrintWriter pw) throws IOException, SQLException{	    
	    if(aFile.isFile())
	    {
	    	pw.println(aFile.getPath());		    	
	    }
	    else if (aFile.isDirectory()) {	    
	    		String path = aFile.getPath();	    		
		      System.out.println("[DIR] " + path);
		      //we skip the jobs that are not professiona and social science		      
		      File[] listOfFiles = aFile.listFiles();
		      if(listOfFiles!=null) {
		        for (int i = 0; i < listOfFiles.length; i++)
		        	getAllFilePaths(listOfFiles[i], pw);
		      } else {
		        System.out.println(" [ACCESS DENIED]");
		      }	 	      	      
	    }
	  }
	
	public static void extractAllXMlPaths(String [] argv) throws IOException, SQLException
	{
		PrintWriter pw = new PrintWriter(new FileWriter(argv[1]));
		File startFile = new File(argv[0]);
		getAllFilePaths(startFile, pw);
		pw.flush();
		pw.close();
	}
	
	public static void checkEmptyFolders(File aFile) throws IOException, SQLException{	    
	    if (aFile.isDirectory()) {	    	    		    				    
		      //we skip the jobs that are not professiona and social science		      
		      File[] listOfFiles = aFile.listFiles();
		      if(listOfFiles!=null) {
		        if(listOfFiles.length == 0)
		        	System.out.println("Empty folder" + aFile.getPath());
		    	  for (int i = 0; i < listOfFiles.length; i++)
		    		  checkEmptyFolders(listOfFiles[i]);
		      }	 	      	      
	    }
	  }
	
	public static void getAllSubFilesFromFolderList(String inputFolderListFname, String fname) throws IOException, SQLException{
		ArrayList<String> fnames = new ArrayList<String>();
		BufferedReader br = new BufferedReader(new FileReader(inputFolderListFname));
		String tmp ="";
		while((tmp = br.readLine()) != null)
		{
			fnames.add(tmp);
		}
		br.close();
		
		PrintWriter pw = new PrintWriter(new FileWriter(fname));
		for(int i=0;i<fnames.size();i++)
		{
			File f = new File(fnames.get(i));
			File[] listOfFiles = f.listFiles();
			  if(listOfFiles!=null) {
			        for (int j = 0; j < listOfFiles.length; j++)
			        	if(listOfFiles[j].isFile())
			        		pw.println(listOfFiles[j].getPath() + "\n");
			  }
		}
		pw.flush();
		pw.close();
		
	  }
	
	public String uncoverHTML(String str)
	{
		String ret = StringUtils.strip(str);
		ret = ret.substring(9, ret.length()- 3);		
		String ret1 = ret.replaceAll("&#\\d+", "");
		ret1 = StringEscapeUtils.unescapeHtml3(ret1);
		
		return ret1;
	}
	
	public ArrayList<WebDoc> parse_xml(String fname) throws IOException
	{
		ArrayList<WebDoc> docList = new ArrayList<WebDoc>();
		BufferedReader br = new BufferedReader(new FileReader(fname));
		String buffer = "";
		WebDoc curDoc = null;
		while((buffer = br.readLine()) != null)
		{
			if(buffer.startsWith("</WEB:WEBRESULT>"))
			{
				docList.add(curDoc);
			}
			else if(buffer.startsWith("<WEB:TITLE>"))
			{
				WebDoc d = new WebDoc();				
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
	
	public ArrayList<WebDoc> parse_xml_for_url(String fname, TreeSet<String> urls) throws IOException
	{
		ArrayList<WebDoc> docList = new ArrayList<WebDoc>();
		BufferedReader br = new BufferedReader(new FileReader(fname));
		String buffer = "";
		WebDoc curDoc = null;
		while((buffer = br.readLine()) != null)
		{
			if(buffer.startsWith("</WEB:WEBRESULT>"))
			{
				if(urls.contains(curDoc.url+curDoc.path))
				{
					docList.add(curDoc);
					System.out.println("[DONE] " + curDoc.url);						
				}
			}
			else if(buffer.startsWith("<WEB:TITLE>"))
			{
				WebDoc d = new WebDoc();				
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
	
	public ArrayList<WebDoc> parse_xml_url_rank(String fname) throws IOException
	{
		ArrayList<WebDoc> docList = new ArrayList<WebDoc>();
		BufferedReader br = new BufferedReader(new FileReader(fname));
		String buffer = "";
		WebDoc curDoc = null;
		int n = 0;
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
				WebDoc d = new WebDoc();				
				curDoc = d;
				curDoc.path = fname;
				curDoc.rank = n;
				buffer = br.readLine();
				//curDoc.title = uncoverHTML(buffer);
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
	
	public void insertDB(ArrayList<WebDoc> docList) throws SQLException
	{			
		for(int i=0;i<docList.size();i++)
		{
			WebDoc d = docList.get(i);
			try
			{	
				
				stmt.setEscapeProcessing(true);
				stmt.clearParameters();
				
				stmt.setString(1, d.url);
				stmt.setString(2, d.title);
				stmt.setString(3, d.desc);
				stmt.setString(4, d.content);
				String text = parseText(d.content);
				stmt.setString(5, text);				
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
	
	/*
	 * Extract the files under depth depth_threshold and output them int a flat list file
	 */
	public static void get_files(String base, String depth_threshold, String output) throws IOException, InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException {
		// TODO Auto-generated method stub		
		Extractor e = new Extractor();
		e.init();
		System.out.println("init");
		ArrayList<String> fnames = new ArrayList<String>();		
		e.getAllFiles(new File(base), fnames, 0, Integer.parseInt(depth_threshold));
		PrintWriter pw = new PrintWriter(new FileWriter(output));
		for(int i = 0;i<fnames.size();i++)
		{
			pw.println(fnames.get(i));
		}
		pw.close();
		e.close();
	}
	
	public void insertIntoDBFromFiles(ArrayList<String> fnames) throws IOException, SQLException
	{
		for(int i = 0;i<fnames.size();i++)
		{
			 System.out.println("[FILE] " + fnames.get(i));	 
			ArrayList<WebDoc> docList = parse_xml(fnames.get(i));
			insertDB(docList);	
		}
	}
	
	/*
	 * Read a list of path name
	 * extract the text in html file and insert them into DB
	 * 
	 */
	public static void get_files(String[] args) throws IOException, InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException {
		// TODO Auto-generated method stub		
		
		BufferedReader br = new BufferedReader(new FileReader(args[0]));
		String tmp = null;
		ArrayList<String> fnames = new ArrayList<String>();
		while((tmp = br.readLine()) != null)
		{
			fnames.add(StringUtils.strip(tmp));
		}
		br.close();
		
		Extractor e = new Extractor();
		e.init();		
		e.insertIntoDBFromFiles(fnames);
		e.close();
	}
	
	
	public void insertIntoDBFromEmptyFiles(ArrayList<String> fnames, TreeSet<String> urls) throws IOException, SQLException
	{
		for(int i = 0;i<fnames.size();i++)
		{
			System.out.println("[FILE] " + fnames.get(i));	 
			ArrayList<WebDoc> docList = parse_xml_for_url(fnames.get(i), urls);
			insertDB(docList);	
		}
	}
	
	public static void get_empty_files(String[] args) throws IOException, InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException {
		// TODO Auto-generated method stub		
		TreeSet<String> urls = new TreeSet<String>();
		BufferedReader br = new BufferedReader(new FileReader(args[0]));
		String tmp = null;
		ArrayList<String> fnames = new ArrayList<String>();
		while((tmp = br.readLine()) != null)
		{
			int start_pos = tmp.indexOf("|") + 1;
			int end_pos = tmp.lastIndexOf("|");
			String url = tmp.substring(start_pos, end_pos);
			String path = tmp.substring(end_pos+1);
			fnames.add(path);
			urls.add(url+path);			
		}
		br.close();
		
		Extractor e = new Extractor();
		e.init();		
		e.insertIntoDBFromEmptyFiles(fnames, urls);
		e.close();
	}
	
	public static String parseText(String html)
	{	
		//Source source=new Source(html);
		//source.fullSequentialParse();		
		//return source.getTextExtractor().setIncludeAttributes(false).toString();
		return Jsoup.parse(html).text();
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
		/*
		Extractor e = new Extractor();
		e.init();
		System.out.println("init");
		e.Process(new File(e.base_folder));
		e.close();
		*/
		//String [] ags = new String [1];
		//ags[0] = "/home/xiao/workspace/Extractor/tmp.txt";
		//get_files(args);
		String [] arg1 = new String [2];
		//arg1[0] = "/home/xiao/workspace/Extractor/v.txt";
		arg1[0] = "/home/xiao/wiki_subject_hierarchy";
		arg1[1] = "all_xml.txt";
		//getAllSubFilesFromFolderList(args[0], args[1]);
		//getAllSubFilesFromFolderList(arg1[0], arg1[1]);
		//checkEmptyFolders(new File(args[0]));		
		//get_files(args);
		String [] arg2 = new String [2];
		//arg1[0] = "/home/xiao/workspace/Extractor/v.txt";
		//arg2[0] = "/home/xiao/workspace/Extractor/local_query_bing_empty_text_id_url_path_list.txt";
		arg2[0] = "/home/xiao/workspace/Extractor/tmp2.txt";
		
		get_empty_files(arg2);
		//get_files(args[0], args[1], args[2]);
	}

}
