package ca.uwo.seeu.hadoop.features;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import java.sql.Connection;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.html.HtmlParser;
import org.apache.tika.sax.BodyContentHandler;
import org.apache.tika.sax.LinkContentHandler;
import org.apache.tika.sax.TeeContentHandler;
import org.apache.tika.sax.ToHTMLContentHandler;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;

import com.mysql.jdbc.Statement;

public class HTMLReader {

	public String readHTMLFromFile(String fname) throws IOException
	{
		BufferedReader br = new BufferedReader(new FileReader(fname));
		String line = "";
		StringBuilder content = new StringBuilder();
		while((line = br.readLine()) != null)
		{
			content.append(line);
			content.append(" ");
		}
		br.close();
		return content.toString();
	}
	
	public static String join(Collection s, String delimiter) {
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
	
	public String readHTMLFromDB(Connection conn, Statement stmt, String schema, String tb, 
			String keytype, String fields, List<String> subList, Hashtable<String, String> docs) throws SQLException
	{
		String content = "";
		
		String sql = "select "+fields+" from " + schema + "."
				+ tb + " where id in (" + join(subList, ",")
				+ ")";
		//System.out.println(sql);
		ResultSet rs = null;
		try {
			rs = stmt.executeQuery(sql);
		} catch (Exception ex) {
			// close database connection
			System.out.println("Database Exception. Try to reset connection");
			ex.printStackTrace();
		}
		// Loop through the result set
		while (rs.next()) {
			// get doc ID, this is a digit number read from DB
			String id = rs.getString(1);
			if(keytype.equals("string"))
				id = "\"" + id + "\"";//append string quote
			String url = rs.getString(2);
			String html = rs.getString(3);

			docs.put(id, url+"<<>>"+html);
		}
		if(rs != null)
			rs.close();
		
		return content;
	}
	
	public void extractHTMLText(String html, Hashtable<String, String> features) throws IOException, SAXException, TikaException
	{
		LinkContentHandler linkHandler = new LinkContentHandler();
        ContentHandler textHandler = new BodyContentHandler(-1);
        ToHTMLContentHandler toHTMLHandler = new ToHTMLContentHandler();
        TeeContentHandler teeHandler = new TeeContentHandler(linkHandler, textHandler, toHTMLHandler);
        
        Metadata metadata = new Metadata();
        ParseContext parseContext = new ParseContext();
        HtmlParser parser = new HtmlParser();
        
        InputStream input = IOUtils.toInputStream(html);
        parser.parse(input, teeHandler, metadata, parseContext);
        
        String [] properties = metadata.names();
        for(int i=0;i<properties.length;i++)
        	System.out.println(properties[i]);
        
        String title = metadata.get("title");
        String keywords = metadata.get("keywords");
        String description = metadata.get("description");
        String body = textHandler.toString();
        
        if(title == null)
        	title = "";
        if(keywords == null)
        	keywords = "";
        if(description == null)
        	description = "";
        if(body == null)
        	body = "";
        
        System.out.println("title: " + title);
        System.out.println("keywords: " + keywords);
        System.out.println("description: " + description);
        
        features.put("title", title);
        features.put("keywords", keywords);
        features.put("description", description);
        features.put("description", description);
        
	}
	
	public void extractHTMLTextByRegex(String html)
	{
		String lowerHTML = html.toLowerCase();
				
		//get title
		int index1 = lowerHTML.indexOf("<title>");
		int index2 = lowerHTML.indexOf("</title>");
		String title="";
		if(index1!=-1 && index2!=-1)
			title = html.substring(index1+7, index2);
		
		System.out.println(title);
		
		//get meta keywords 
		Pattern pattern = Pattern.compile("<meta\\s+name=[\"|']?keywords[\"|']?\\s+content=[\"|']?([^\"]*)[\"|']?");
		Matcher matcher = pattern.matcher(html);
		String keywords = "";
		if(matcher.find())
		{
			keywords = matcher.group();
			System.out.println("1" + keywords);
		}
		else
		{
			pattern = Pattern.compile("<meta\\s+content=[\"|']?([^\"]*)[\"|']?\\s+name=[\"|']?keywords[\"|']?");
			matcher = pattern.matcher(html);
			if(matcher.find())
			{
				keywords = matcher.group();
				System.out.println("2" + keywords);
			}
			
		}
		
		//get meta description 
		pattern = Pattern.compile("<meta\\s+name=[\"|']?description[\"|']?\\s+content=[\"|']?([^\"]*)[\"|']?");
		matcher = pattern.matcher(html);
		String description = "";
		if(matcher.find())
		{
			description = matcher.group();
			System.out.println("1" + description);
		}
		else
		{
			pattern = Pattern.compile("<meta\\s+content=[\"|']?([^\"]*)[\"|']?\\s+name=[\"|']?description[\"|']?");
			matcher = pattern.matcher(html);
			if(matcher.find())
			{
				description = matcher.group();
				System.out.println("2" + description);
			}
			
		}
		
		//get body text
		String body = "";
		int index3 = lowerHTML.indexOf("<body");
		if(index3 != -1)
			body = html.substring(index3);
		else
			body = html;
		String contentWithHTML = StringEscapeUtils.escapeHtml4(body);
		System.out.println(contentWithHTML);
		
		//keywords=re.search("<meta\s+name=[\"|\']?keywords[\"|\']?\s+content=[\"|\']?([^\"]*)[\"|\']?", ct,2)
		//if keywords==None:
		//	keywords=re.search("<meta\s+content=[\"|\']?([^\"]*)[\"|\']?\s+name=[\"|\']?keywords[\"|\']?", ct,2)
		
		
	}
	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		HTMLReader h = new HTMLReader();
		String html = h.readHTMLFromFile("/home/xiao/Downloads/uwo.html");
		h.extractHTMLTextByRegex(html);
	}	

}
