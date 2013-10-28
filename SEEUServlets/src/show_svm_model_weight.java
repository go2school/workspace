import javax.servlet.*;
import javax.servlet.http.*;

import com.mysql.jdbc.Statement;

import java.io.*;
import java.sql.Connection;
import java.util.*;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;

public class show_svm_model_weight extends HttpServlet
{		
	private String schema = "query_search_engine";
	public String pos_word_table = "svm_model_positive_raw_words";
	public String neg_word_table = "svm_model_negative_raw_words";	
	
	private String userName = "root";
	private String password = "see";	
	private String dburl = "jdbc:mysql://192.168.0.2:3306/query_search_engine?characterEncoding=UTF-8";          
    private Statement stmt = null;    
    private Connection conn = null;
     
    public void init()
    {    	    
    	try
    	{
	    	Class.forName ("com.mysql.jdbc.Driver").newInstance ();
	        conn = DriverManager.getConnection (dburl, userName, password);	        
    	}
    	catch(Exception ex)
    	{
    		System.out.println(ex);
    	}
    }
    
    public void destroy()
    {
    	if(stmt != null)
        {
      	  try
            {            	  
                stmt.close ();
            }
            catch (Exception e) { System.err.println(e); }
        }
        if (conn != null)
        {
            try
            {                        	  
                conn.close ();                
            }
            catch (Exception e) {System.err.println(e); }
        }        
    }
       
    public void queryModelWordWeight(int mid, String table, LinkedList<String> words, LinkedList<String> weights)
    {
    	String sql = "select word_weight from "+schema+"."+table+" where id=" + mid;
    	init();    	    	
    	ResultSet rs = null;
    	try
	    {	      
    	  stmt = (Statement) conn.createStatement();
	      rs = stmt.executeQuery(sql);
	      String word_weight = "";	      
	      while (rs.next())
	      {	    	  	    	 
	    	  word_weight = rs.getString(1);	    	  
	      }
	      rs.close();
	      
	      String [] old_words = word_weight.split(" ");	      
	      for(int i=0;i<old_words.length;i++)
	      {
	    	  String [] tmp = old_words[i].split(":");
	    	  //words.add(tmp[0].replace("_b", "").replace("_t", ""));
	    	  words.add(tmp[0]);
	    	  double weight = Double.parseDouble(tmp[1]);
	    	  weights.add(""+Math.abs(weight));	    	  
	      }	     	     	      	   
	    }
	    catch (SQLException ex)
	    {
	    	System.out.println(ex);
	    } 
    	destroy();
    }
       
    public String joinStringList(LinkedList<String> words, LinkedList<String> weights)
    {    
    	String ret = "\"" + words.get(0) + "\":\"" + weights.get(0) + "\"";
    	for(int i=1;i<words.size();i++)
    	{
    		ret += "," + "\"" + words.get(i) + "\":\"" + weights.get(i) + "\"";
    	}
    	return ret;
    }
    
    public String writeJSON(int [] mids)
    {
    	String json_str = "numFound = {";
		
		for(int i = 0;i<mids.length;i++)
		{
			int n_model_id = mids[i]; 

	    	//get positive word weight
			LinkedList<String> pos_words = new LinkedList<String>();     	
	    	LinkedList<String> pos_weights = new LinkedList<String>();	    	
	    	queryModelWordWeight(n_model_id, pos_word_table, pos_words, pos_weights);
	    	//get negative word weight
	    	LinkedList<String> neg_words = new LinkedList<String>();     	
	    	LinkedList<String> neg_weights = new LinkedList<String>();	    	
	    	queryModelWordWeight(n_model_id, neg_word_table, neg_words, neg_weights);
	    	
	    	System.out.println("Get model " + n_model_id);
	    	
	    	if(pos_words.size() == 0 || neg_words.size() == 0)
	    	{
	    		System.out.println("Read db error");
	    		return null;
	    	}
	    	//make json string
	    	String json_pos_str = joinStringList(pos_words, pos_weights);
	    	String json_neg_str = joinStringList(neg_words, neg_weights);
	    	
	    	String model_str = "\"" + n_model_id + "\":[{" + json_pos_str + "},{" + json_neg_str + "}]";
	    	
	    	if(i == 0)
	    		json_str += model_str;
	    	else
	    		json_str += "," + model_str;
		}
		json_str += "}";
		
		return json_str;
    }
    
    public void doGet(HttpServletRequest req, HttpServletResponse resp)
               throws ServletException,IOException
    {	
    	//get model id parameter
    	String midParam = "mid";
		String mid = req.getParameter(midParam);				
	
		if(mid == null)
		{
			return;
		}	
		
		String [] mids = mid.split("_");		
		int [] int_mids = new int [mids.length];
		for(int i=0;i<mids.length;i++)
			int_mids[i] = Integer.parseInt(mids[i]);		
		
		/*
		 * data format
		 * {
		 * "0": [
		        {//positive
					"v" : "aed",			
		            "w": "0.3234",
		            "lid": "12",
		            "gid": "1234"
		        },
		        {//negative
		            "aed_b": "122",
		            "ades_b": "42423",
		            "aing_b": "13131"
		        }
    			],
    		}
		 */
		
		String json_str = writeJSON(int_mids);
		
		resp.addHeader("Content-Type", "text/javascript");
		resp.addHeader("Access-Control-Allow-Origin", "*");			
				
		java.io.PrintWriter pw=resp.getWriter();
		pw.write(json_str);								
		pw.close();			
	}
    
    public static void main(String[] args) throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException, IOException
    {    	
    	show_svm_model_weight s = new show_svm_model_weight();
    	s.init();
    	//s.queryModelWordWeight(0, s.pos_word_table, words, weights);
    	int [] ids = new int [3];
    	ids[0] = 1138;
    	ids[1] = 1145;
    	ids[2] = 1159;
    	String str = s.writeJSON(ids);
    	System.out.println(str);
    	s.destroy();    	
    }
}
