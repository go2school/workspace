import java.io.IOException;
import java.sql.SQLException;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CountRegYear {

	public DBUtility db= null;
	public void setDBUtility()
	{
		
	}
	
	public Hashtable<String, Integer> countYear(String text)
	{
		Pattern pattern = Pattern.compile("(19|20)\\d\\d");
		Matcher matcher = pattern.matcher(text);
		Hashtable<String, Integer> data = new Hashtable<String, Integer>();
		// Check all occurance
		while (matcher.find()) {			
			String g = matcher.group();
			if(data.containsKey(g))
			{
				data.put(g, data.get(g)+1);
			}
			else
				data.put(g,1);
		}
		return data;
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
		// TODO Auto-generated method stub
		CountRegYear c = new  CountRegYear();
		c.db = new DBUtility();
		c.db.init("uwo");
		
		Hashtable<String, Integer> data = c.countYear(c.db.queryDBGetField("167968", "content", "uwo_new_nutch_docs"));
		Enumeration<String> e = data.keys();
		while(e.hasMoreElements())
		{
			String k = e.nextElement();
			System.out.println(k + " " + data.get(k));
		}
		
		c.db.close();
	}

}
