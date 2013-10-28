package ca.uwo.see.learn;
import javax.servlet.*;
import javax.servlet.http.*;
import java.io.*;
import java.util.*;

public class SpellCorrector extends HttpServlet
{		   			
	public String dict_fname = "";
	
	public static SymDelSpellChecker ssc = new SymDelSpellChecker();
	
	public void init() throws ServletException {	
	     //load dictionary
		 	
		dict_fname = getServletConfig().getInitParameter("dict");
		 System.out.println(dict_fname);
		 try {
			ssc = SymDelSpellChecker.learnSpellChecker(dict_fname);
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	 }
	
	public void doGet(HttpServletRequest req, HttpServletResponse resp)
               throws ServletException,IOException
    {	
		String word=req.getParameter("q");
		String num=req.getParameter("n");		
		
		int num_sug = Integer.parseInt(num);
		
		ArrayList<suggestItem> sug = ssc.getCorrector(word);
				
		//make json ouput
		String output = "suggestions = {";			
		int j= 0;//return num_sug results
		for(int i=0;i<sug.size() && j < num_sug;i++)
		{
			//ignore similar word
			if(sug.get(i).term.equals(word) == false)
			{
				output += "\"" + sug.get(i).term + "\":[" + sug.get(i).count + "," + sug.get(i).distance + "," + sug.get(i).metaphone_distance + "],";
				j++;
			}
		}				
		output += "}";
		
		System.out.println(word + " " + num + " " + output);
		
		resp.addHeader("Content-Type", "text/javascript");
		resp.addHeader("Access-Control-Allow-Origin", "*");			
				
		java.io.PrintWriter pw=resp.getWriter();
		pw.write(output);								
		pw.close();	
	}
	
	public static void main(String argv [] )
	{
		System.out.println("spell corrector");
	}
	
}
