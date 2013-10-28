import java.sql.*;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;

public class SEEUAnalytics {

	class MyObject2 implements Comparable 
	{
		public MyObject2() {  
		    } 
		 
		public String key = "";
		public int value = 0;
		@Override
		public int compareTo(Object arg0) {
			// TODO Auto-generated method stub
			MyObject2 m = (MyObject2)arg0;
			return m.value - this.value;
		}
	}
	
	class MyObject 
	{
		public String key = "";
		public int value = 0;
	}
	class CustomComparator implements Comparator<MyObject> {
	    @Override
	    public int compare(MyObject o1, MyObject o2) {
	        return o2.value - o1.value;
	    }
	}
		
	public ArrayList<MyObject2> getTopQuery(HashMap<String, Integer> queryCount)
	{
		Set<String> keys = queryCount.keySet();
		Iterator<String> it = keys.iterator();
		ArrayList<MyObject2> vlst = new ArrayList<MyObject2>();
		while(it.hasNext())
		{
			String s = it.next();
			MyObject2 m = new MyObject2();
			m.key = s;
			m.value = queryCount.get(s);	
			vlst.add(m);
		}
		
		Collections.sort(vlst);
		
		return vlst;		
	}
	
	public HashMap<String, Integer> getFieldCountBetweenDate(Statement stmt, String keyField, String Mainfield, String datefield, String schema, String tb, String [] datefrom, String [] dateto) throws SQLException
	{
		///y, m, d
		String fromdate = datefrom[1] + "/" + datefrom[2] + "/" + datefrom[0];
		String todate = dateto[1] + "/" + dateto[2] + "/" + dateto[0];
		String sql = "SELECT "+keyField+","+Mainfield+" FROM "+schema+"."+tb+" WHERE "+datefield+" >= STR_TO_DATE('"+fromdate+"', '%m/%d/%Y') AND "+datefield+" < STR_TO_DATE('"+todate+"', '%m/%d/%Y');";
		ResultSet res = stmt.executeQuery(sql);
		String id = "";
		String query = "";	
		HashMap<String, Integer> queryCount = new HashMap<String, Integer>();
		while(res.next())
		{	
			id = res.getString(keyField);	
			query = res.getString(Mainfield);	
			if(queryCount.containsKey(query))
			{
				Integer v = queryCount.get(query);
				v = v + 1;
				queryCount.put(query, v);
			}
			else
			{
				queryCount.put(query, new Integer(1));
			}						
		}
		return queryCount;
	}
	
	String join(Collection s, String delimiter) {
		StringBuffer buffer = new StringBuffer();
		Iterator iter = s.iterator();
		while (iter.hasNext()) {
			buffer.append(iter.next());
			if (iter.hasNext()) {
				buffer.append(delimiter);
			}
		}
		return buffer.toString();
	}
	
	//query between date and return multipel fileds
	public ArrayList<ArrayList<String>> getFieldsBetweenDate(Statement stmt, String keyField, ArrayList<String> Mainfields, String datefield, String schema, String tb, String datefrom, String dateto) throws SQLException
	{
		///y, m, d		
		String sql = "SELECT "+keyField+","+join(Mainfields, ",")+" FROM "+schema+"."+tb+" WHERE "+datefield+" >= '" + datefrom+" 00:00:00' AND "+datefield+" < '"+dateto+" 00:00:00' order by id asc;";
		ResultSet res = stmt.executeQuery(sql);
		String id = "";		
		ArrayList<ArrayList<String>> queryCount = new ArrayList<ArrayList<String>>();
		while(res.next())
		{	
			ArrayList<String> row = new ArrayList<String>();	
			row.add(res.getString(1));
			for(int i=0;i<Mainfields.size();i++)
			{
				row.add(res.getString(2 + i));
			}
			queryCount.add(row);						
		}
		return queryCount;
	}
	
	//query field by value
	public ArrayList<ArrayList<String>> getFieldsBetweenDateByFieldValue(Statement stmt, String keyField, String whereKeytype, String whereKeyField, String whereKeykeyFieldValue, ArrayList<String> Mainfields, String datefield, String schema, String tb, String datefrom, String dateto) throws SQLException
	{
		///y, m, d		
		String sql = "";
		if(whereKeytype.equals("string"))
			sql = "SELECT "+keyField+","+join(Mainfields, ",")+" FROM "+schema+"."+tb+" WHERE " +whereKeyField + "=\"" + whereKeykeyFieldValue + "\" and " + datefield+" >= '" + datefrom+" ' AND "+datefield+" < '"+dateto+"' order by time asc;";
		else
			sql = "SELECT "+keyField+","+join(Mainfields, ",")+" FROM "+schema+"."+tb+" WHERE " +whereKeyField + "=" + whereKeykeyFieldValue + "\" and " +  datefield+" >= '" + datefrom+" ' AND "+datefield+" < '"+dateto+" ' order by time asc;";
		ResultSet res = stmt.executeQuery(sql);
		String id = "";		
		ArrayList<ArrayList<String>> queryCount = new ArrayList<ArrayList<String>>();
		while(res.next())
		{	
			ArrayList<String> row = new ArrayList<String>();	
			row.add(res.getString(1));
			for(int i=0;i<Mainfields.size();i++)
			{
				row.add(res.getString(2 + i));
			}
			queryCount.add(row);						
		}
		return queryCount;
	}
	
	public void getSessionDurationByTime(ArrayList<ArrayList<String>> para, HashMap<String, Timestamp> startTime, HashMap<String, Timestamp> endTime)
	{
		HashMap<String, ArrayList<Timestamp>> sessionDuration = new HashMap<String, ArrayList<Timestamp>>();
		for(int i=0;i<para.size();i++)
		{
			String rid = para.get(i).get(0);
			String sid = para.get(i).get(1);
			String time = para.get(i).get(2);
			
			if(sessionDuration.containsKey(sid))
			{
				ArrayList<Timestamp> o = sessionDuration.get(sid);
				o.add(Timestamp.valueOf(time));
			}
			else
			{
				ArrayList<Timestamp> o = new ArrayList<Timestamp>();
				o.add(Timestamp.valueOf(time));
				sessionDuration.put(sid, o);
			}
		}
		
		//compute start and end time
		Set<String> keys = sessionDuration.keySet();
		Iterator<String> it = keys.iterator();
		while(it.hasNext())
		{
			String k = it.next();
			ArrayList<Timestamp> times = sessionDuration.get(k);
			//sort timestamp
			Collections.sort(times);
			//get minimal and maximal timestamp
			Timestamp mintime = times.get(0);
			Timestamp maxtime = times.get(times.size() - 1);
			
			startTime.put(k, mintime);
			endTime.put(k, maxtime);
		}
	}
	
	public String changeTimeFormat(String in)
	{
		//change format
		in = in.replace(".0","");		
		
		int p1 = in.indexOf(" ");
		int p2 = in.indexOf(":");
		String t1 = in.substring(p1+1, p2);
		int t1_hour = Integer.parseInt(t1);
		String t1_ext = "";
		if(t1_hour >= 12)
			t1_ext = "PM";
		else
			t1_ext = "AM";
		return in + t1_ext;
	}
	/**
	 * @param args
	 * @throws ClassNotFoundException 
	 * @throws IllegalAccessException 
	 * @throws InstantiationException 
	 * @throws SQLException 
	 */
	public static void main(String[] args) throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException {
		// TODO Auto-generated method stub

		//print  hashmap
		SEEUAnalytics s = new SEEUAnalytics();
		
		//get all query between date
		String dburl = "jdbc:mysql://192.168.0.2:3306/";
		String driver = "com.mysql.jdbc.Driver";
		String userName = "root"; 
		String password = "see";
		String dbName = "uwestern2";
		
		Class.forName(driver).newInstance();
		Connection conn = DriverManager.getConnection(dburl+dbName,userName,password);				

		Statement stmt = conn.createStatement();
		
		String keyField ="id";
		String Mainfield = "query";
		String datefield = "time";
		String whereKeyField = "sessionid";
		String schema = "seeu";
		String tb = "link_click_logs";
		String [] datefrom = {"2012", "11", "07"};//y, m, d
		String [] dateto = {"2013", "03", "11"};
		
		//HashMap<String, Integer> ret = s.getFieldCountBetweenDate(stmt, keyField, Mainfield, datefield, schema, tb, datefrom, dateto);
		ArrayList<String> Mainfields = new ArrayList<String>();
		Mainfields.add("sessionid");
		Mainfields.add("time");
		
		ArrayList<ArrayList<String>> ret = s.getFieldsBetweenDate(stmt, keyField, Mainfields, datefield, schema, tb, "2012-12-29", "2012-12-29");
		HashMap<String, Timestamp> startTime = new HashMap<String, Timestamp>();
		HashMap<String, Timestamp> endTime = new HashMap<String, Timestamp>();
		s.getSessionDurationByTime(ret, startTime, endTime);
		
		Set<String> keys = startTime.keySet();
		Iterator<String> it = keys.iterator();
		while(it.hasNext())
		{
			String st = it.next();
			long a = startTime.get(st).getTime();
			long b = endTime.get(st).getTime();
			
			String start_time = startTime.get(st).toString();
			String end_time = endTime.get(st).toString();
			
			//change format
			start_time = s.changeTimeFormat(start_time);
			end_time = s.changeTimeFormat(end_time);
			
			System.out.println(st + " " + start_time + " " + end_time + " " + (b - a)/1000);
		}
		
		
		Mainfields.clear();
		Mainfields.add("sessionid");
		Mainfields.add("time");
		Mainfields.add("curcat");
		Mainfields.add("pageid");
		Mainfields.add("link");
		Mainfields.add("query");
		
		ArrayList<ArrayList<String>> ret2 = s.getFieldsBetweenDateByFieldValue(stmt, keyField, "string", whereKeyField, "13mbllo1nez7k", Mainfields, datefield, schema, tb, "2012-11-20 00:00:00", "2013-01-10 12:23:00");
		for(int i=0;i<ret2.size();i++)
		{
			String v = "";			
			for(int j=0;j<ret2.get(i).size();j++)
				v += ret2.get(i).get(j)  + "   ";
			System.out.println(v);
		}
		
		
		/*
		Set<String> keys = startTime.keySet();
		Iterator<String> it = keys.iterator();
		while(it.hasNext())
		{
			String st = it.next();
			long a = startTime.get(st).getTime();
			long b = endTime.get(st).getTime();
			
			String start_time = startTime.get(st).toString();
			String end_time = endTime.get(st).toString();
			
			//change format
			start_time = s.changeTimeFormat(start_time);
			end_time = s.changeTimeFormat(end_time);
			
			System.out.println(st + " " + start_time + " " + end_time + " " + (b - a)/1000);
		}
		*/
		
		
		stmt.close();
		conn.close();	
		
		//s.getTopQuery(ret);		
	}

}
