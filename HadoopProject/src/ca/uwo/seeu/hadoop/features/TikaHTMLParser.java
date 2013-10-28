package ca.uwo.seeu.hadoop.features;

import java.util.List;
import java.io.InputStream;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.Hashtable;

import org.apache.commons.io.IOUtils;
import org.apache.tika.Tika;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.html.HtmlParser;
import org.apache.tika.sax.BodyContentHandler;
import org.apache.tika.sax.Link;
import org.apache.tika.sax.LinkContentHandler;
import org.apache.tika.sax.TeeContentHandler;
import org.apache.tika.sax.ToHTMLContentHandler;
import org.xml.sax.ContentHandler;

import com.mysql.jdbc.Statement;

public class TikaHTMLParser {

	
    public static void main (String args[]) throws Exception {
        //URL url = new URL("http://www.mansfield.ohio-state.edu/~sabedon/mpb/mpb_literature_search_ab.htm");
    	URL url = new URL("http://www.csd.uwo.ca");
        InputStream input = url.openStream();
        
        LinkContentHandler linkHandler = new LinkContentHandler();
        ContentHandler textHandler = new BodyContentHandler(-1);
        ToHTMLContentHandler toHTMLHandler = new ToHTMLContentHandler();
        TeeContentHandler teeHandler = new TeeContentHandler(linkHandler, textHandler, toHTMLHandler);
        
        Metadata metadata = new Metadata();
        ParseContext parseContext = new ParseContext();
        HtmlParser parser = new HtmlParser();
        
        /*
        Connection conn = DriverManager.getConnection("jdbc:mysql://192.168.0.2:3306/", "root", "see");
		Statement stmt = (Statement) conn.createStatement();
		String schema = "query_search_engine";
		String tb = "webdoc";
		String fields = "id,url,html";
		String keytype = "int";		
		ArrayList<String> list = new ArrayList<String>();
		list.add("1");
		List<String> subList = list.subList(0, 1);		
		HTMLReader hr = new HTMLReader();
		Hashtable<String, String> docs = new Hashtable<String, String>();
		hr.readHTMLFromDB(conn, stmt, schema, tb, keytype, fields, subList, docs);		
		String source = docs.get("1");
		//InputStream input = IOUtils.toInputStream(source);
		*/             
        
        parser.parse(input, teeHandler, metadata, parseContext);
               
        String [] properties = metadata.names();
        for(int i=0;i<properties.length;i++)
        	System.out.println(properties[i]);
        
        String title = metadata.get("title");
        String keywords = metadata.get("keywords");
        String description = metadata.get("description");
       
        String body = textHandler.toString();
        
        System.out.println("title: " + title);
        System.out.println("keywords: " + keywords);
        System.out.println("description: " + description);
        //System.out.println("body: " + body);
       
        
        List<Link> links = linkHandler.getLinks();
        for(Link l : links)
        {
        	URL newURL = new URL(url, l.getUri());
        	if(l.isAnchor())
        		System.out.println(newURL.toString() + " | " + l.getText() );
        	
        } 
               
        //System.out.println("text:\n" + textHandler.toString());
        //System.out.println("html:\n" + toHTMLHandler.toString());
    }
}