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

public class ExtractAllSubFiles {
	
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
	
	public static void getAllFilePathOnlyOneLevel(File aFile, PrintWriter pw) throws IOException, SQLException{	    
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
		        {
		        	pw.println(listOfFiles[i].getPath());
		        }		       
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
		
	public static void extractAllXMlPathsInFolders(String [] argv) throws IOException, SQLException
	{
		String inputFname = argv[0];
		String outputFname = argv[1];
		PrintWriter pw = new PrintWriter(new FileWriter(outputFname));
		BufferedReader br = new BufferedReader(new FileReader(inputFname));
		String tmp = "";
		while((tmp = br.readLine()) != null)
		{
			File startFile = new File(tmp);
			getAllFilePathOnlyOneLevel(startFile, pw);
		}
		br.close();		
		pw.flush();
		pw.close();
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
		
		String [] argvs = new String [2];
		argvs[0] = "/home/xiao/wiki_subject_hierarchy/SEEUWO_Training_Data_top_two";
		argvs[1] = "loca_xml.txt";
		//extractAllXMlPathsInFolders(args);
		extractAllXMlPaths(argvs);
	}

}
