package ca.uwo.seeu;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.w3c.dom.Attr;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

public class ElevateManager {

	public HashMap<String, RuleObj> map = new HashMap<String, RuleObj>();
	
	class RuleObj {
	 	public String query = "";
	 	public String cat = "";	
	 	public String uname = "";
	 	public String file = "";
	    public List<String> elevate  = null;//ordered list of included ids
	    public List<String> exclude = null;//the order does not matter
	    
	    RuleObj( String qstr, String cat, String uname, String file, List<String> elevate, List<String> exclude ) 
	    {	
	    	this.query = qstr;
	    	this.cat = cat;
	    	this.uname = uname;
	    	this.file = file;
	    	this.elevate = elevate;
	    	this.exclude = exclude;
	    }
	  }
	 
	 public String join(Collection s, String delimiter) {
			StringBuffer buffer = new StringBuffer();
			Iterator<String> iter = s.iterator();
			while (iter.hasNext()) {
				buffer.append(iter.next());
				if (iter.hasNext()) {
					buffer.append(delimiter);
				}
			}
			return buffer.toString();
		}
	 
	 public String string_to_sorted(String input, String insep, String outsep)
	  {
		  String [] tmp_lst = input.split(insep);
		  ArrayList<String> tmp = new ArrayList<String>();
			for(int j=0;j<tmp_lst.length;j++)
			{	    					
				tmp.add(tmp_lst[j]);
			}
			Collections.sort(tmp);
			return join(tmp, outsep);	
	  }
	
	 public void writeRules(String fname)
	 {
		 Set<String> keys = map.keySet();
		 Iterator<String> it = keys.iterator();
		 		
		  try {
			  
				DocumentBuilderFactory docFactory = DocumentBuilderFactory.newInstance();
				DocumentBuilder docBuilder = docFactory.newDocumentBuilder();
		 
				// root elements
				Document doc = docBuilder.newDocument();
				Element rootElement = doc.createElement("elevate");
				doc.appendChild(rootElement);
		 
				while(it.hasNext())
				 {
					 String key = it.next();
					 RuleObj rule = map.get(key);
					 
					 Element rule_ele = doc.createElement("query");					 
					 rootElement.appendChild(rule_ele);
					 
					// set attribute to staff element
					Attr text_attr = doc.createAttribute("text");
					Attr cat_attr = doc.createAttribute("cat");
					Attr uname_attr = doc.createAttribute("uname");
					Attr file_attr = doc.createAttribute("file");										
					
					text_attr.setValue(rule.query);
					cat_attr.setValue(rule.cat);
					uname_attr.setValue(rule.uname);
					file_attr.setValue(rule.file);					
					
					rule_ele.setAttributeNode(text_attr);
					rule_ele.setAttributeNode(cat_attr);
					rule_ele.setAttributeNode(uname_attr);
					rule_ele.setAttributeNode(file_attr);					
					
					//create included docs in this rule
					if(rule.elevate != null)
						for(int i=0;i<rule.elevate.size();i++)
						{
							Element d = doc.createElement("doc");
							Attr d_id = doc.createAttribute("id");
							d_id.setValue(rule.elevate.get(i));
							d.setAttributeNode(d_id);
							
							rule_ele.appendChild(d);
						}
					
					//create excluded ids in this rule
					if(rule.exclude != null)
						for(int i=0;i<rule.exclude.size();i++)
						{
							Element d = doc.createElement("doc");
							
							Attr d_id = doc.createAttribute("id");
							d_id.setValue(rule.exclude.get(i));
							d.setAttributeNode(d_id);
							
							Attr d_exclude = doc.createAttribute("exclude");
							d_exclude.setValue("true");
							d.setAttributeNode(d_exclude);
							
							rule_ele.appendChild(d);
						}
				 }
								
				// write the content into xml file
				TransformerFactory transformerFactory = TransformerFactory.newInstance();
				Transformer transformer = transformerFactory.newTransformer();
				DOMSource source = new DOMSource(doc);
				StreamResult result = new StreamResult(new File(fname));
		 		
				transformer.setOutputProperty(OutputKeys.INDENT, "yes");
				transformer.setOutputProperty("{http://xml.apache.org/xslt}indent-amount", "2");
				transformer.transform(source, result);
		
			  } catch (ParserConfigurationException pce) {
				pce.printStackTrace();
			  } catch (TransformerException tfe) {
				tfe.printStackTrace();
			  }
	 }
	 
	 public void readRules(String fname)
	 {		 
		 DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
						
			try {

				//Using factory get an instance of document builder
				DocumentBuilder db = dbf.newDocumentBuilder();

				//parse using builder to get DOM representation of the XML file
				Document dom = db.parse(fname);
				
				Element docEle = dom.getDocumentElement();			
				
				//for each query object build a Rule
				NodeList nl = docEle.getElementsByTagName("query");
				if(nl != null && nl.getLength() > 0) {
					for(int i = 0 ; i < nl.getLength();i++) {

						//get the employee element
						Element el = (Element)nl.item(i);
						
						//get attribute
						String q = el.getAttribute("text");
						String cat = el.getAttribute("cat");
						String uname = el.getAttribute("uname");
						String file = el.getAttribute("file");
						
						cat = string_to_sorted(cat, " ", " ");
						uname = string_to_sorted(uname, " ", " ");
						file = string_to_sorted(file, " ", " ");
						
						ArrayList<String> include_ids = new ArrayList<String>();
						ArrayList<String> exclude_ids = new ArrayList<String>();
						
						NodeList docs = el.getElementsByTagName("doc");
						for(int j=0;j<docs.getLength();j++)
						{			
							Element doc = (Element)docs.item(j);
							String id = doc.getAttribute("id");
							String exclude = doc.getAttribute("exclude");							
							
							if(exclude.equals("true"))
								exclude_ids.add(id);
							else
								include_ids.add(id);
						}
						
						String key = q + "<<>>" + cat + "<<>>" + uname + "<<>>" + file;
						RuleObj rule = new RuleObj(q, cat, uname, file, include_ids, exclude_ids);
						map.put(key, rule);
					}
				}

			}catch(ParserConfigurationException pce) {
				pce.printStackTrace();
			}catch(SAXException se) {
				se.printStackTrace();
			}catch(IOException ioe) {
				ioe.printStackTrace();
			}			
	 }
	 
	 public void resolveElevateAndExcludeKeepAdded(RuleObj r)
	 {
		//if no included ids or excluded ids, just return
		 if(r.elevate == null || r.exclude == null)
			 return;
		 
		 //we prefer the included ids
		 //remove all the excluded ids that appearing in the included ids
		 TreeSet<String> s1 = new TreeSet<String>();
		 TreeSet<String> s2 = new TreeSet<String>();
		 
		 for(int i=0;i<r.elevate.size();i++)
			 s1.add(r.elevate.get(i));
		 for(int i=0;i<r.exclude.size();i++)
			 s2.add(r.exclude.get(i));
		 
		 //get intersection
		 s2.removeAll(s1);
		 r.exclude.clear();
		 Iterator<String> it = s2.iterator();
		 while(it.hasNext())		 
			 r.exclude.add(it.next());		 
	 }
	 
	 public void resolveElevateAndExcludeKeepDeleted(RuleObj r)
	 {
		 //if no included ids or excluded ids, just return
		 if(r.elevate == null || r.exclude == null)
			 return;
		 
		 //we prefer the included ids
		 //remove all the excluded ids that appearing in the included ids
		 TreeSet<String> s1 = new TreeSet<String>();
		 TreeSet<String> s2 = new TreeSet<String>();
		 
		 
		 for(int i=0;i<r.elevate.size();i++)
			 s1.add(r.elevate.get(i));
		 
		 for(int i=0;i<r.exclude.size();i++)
			 s2.add(r.exclude.get(i));
		 
		 //get intersection
		 s1.removeAll(s2);
		 r.elevate.clear();
		 Iterator<String> it = s1.iterator();
		 while(it.hasNext())		 
			 r.elevate.add(it.next());		 
	 }
	 
	 /*
	  * Assuming each para is separated by something
	  * cat: cat_prob_1__cat_prob_2__cat_prob_3
	  * uname: uname__uanem__uname
	  * file: f1__f2__f3
	  */
	 public void addDocRule(String cat_para, String uname_para, String file_para, String query, ArrayList<String> docIds)
	 {
		 if(docIds == null || docIds.size() == 0)
			 return;
		 
		 //make key first
		 cat_para = string_to_sorted(cat_para, "__", " ");
		 uname_para = string_to_sorted(uname_para, "__", " ");
		 file_para = string_to_sorted(file_para, "__", " ");
		 
		 String key = query + "<<>>" + cat_para + "<<>>" + uname_para + "<<>>" + file_para;
		 
		 //check if this key exist
		 if(map.containsKey(key))
		 {
			 RuleObj r = map.get(key);
			 
			 //check if the new id list are longer than the current one
			 //that means the new id list contains all the ids in the old id list
			 //we only keep the longest id list
			 if(docIds.size() > r.elevate.size())
			 {
				 r.elevate = docIds;
			 }			
			 else if(docIds.size() < r.elevate.size())
			 {				 
				 //insert the last doc to the diff position
				 r.elevate.add(docIds.size() - 1, docIds.get(docIds.size() - 1));
			 }
			 else//same length, that means the n-1 docs are the same, only the last is different
			 {
				 //we add it at the last position
				 r.elevate.add(r.elevate.size() - 1, docIds.get(docIds.size() - 1));
			 }
			 
			 //check the newly added docs if exist in the excluded list
			 resolveElevateAndExcludeKeepAdded(r);
		 }
		 else
		 {
			 //create rule
			 RuleObj rule = new RuleObj(query, cat_para, uname_para, file_para, docIds, null);
			 
			 resolveElevateAndExcludeKeepAdded(rule);
			 
			 map.put(key, rule);
		 }
	 }
	 
	 /*
	  * merge the newly deleted ids with the current one
	  * resolve the conflict by favoring the deleted ids
	  */
	 public void deleteDocRule(String cat_para, String uname_para, String file_para, String query, ArrayList<String> docIds)
	 {
		 if(docIds == null || docIds.size() == 0)
			 return;
		 
		//make key first
		 cat_para = string_to_sorted(cat_para, "__", " ");
		 uname_para = string_to_sorted(uname_para, "__", " ");
		 file_para = string_to_sorted(file_para, "__", " ");
		 
		 String key = query + "<<>>" + cat_para + "<<>>" + uname_para + "<<>>" + file_para;
		
     	 //check if this key exist
		 if(map.containsKey(key))
		 {
			 RuleObj r = map.get(key);
			 
			 //merge the excluded id list
			 TreeSet<String> s1 = new TreeSet<String>();
			 for(int i=0;i<docIds.size();i++)
				 s1.add(docIds.get(i));
			 if(r.exclude != null)
			 {
				 for(int i=0;i<r.exclude.size();i++)
					 s1.add(r.exclude.get(i));
				 r.exclude.clear();
			 }			 
			 else
			 {
				 r.exclude = new ArrayList<String>();				 
			 }
			 Iterator<String> it = s1.iterator();
			 while(it.hasNext())		 
				 r.exclude.add(it.next());	
			 
			 resolveElevateAndExcludeKeepDeleted(r);
		 }
		 else
		 {
			//create rule
			 RuleObj rule = new RuleObj(query, cat_para, uname_para, file_para, null, docIds);
			 
			 resolveElevateAndExcludeKeepDeleted(rule);
			 
			 map.put(key, rule);
		 }
	 }
	 
	 public static String  getHTML(String url) throws Exception {
	        URL u = new URL(url);
	        URLConnection yc = u.openConnection();
	        BufferedReader in = new BufferedReader(new InputStreamReader(
	                                    yc.getInputStream()));
	        String inputLine = null;
	        String ret = "";
	        while ((inputLine = in.readLine()) != null) 
	            ret += inputLine;
	        in.close();
	        return ret;
	    }
	 
	 public void copy_file_remotely(String fromFile, String toMachine, String toPath) throws IOException, InterruptedException
	 {
	 	String cmd = "scp -r "+fromFile+" "+toMachine+":" + toPath;
	 	Process p=Runtime.getRuntime().exec(cmd); 
	    p.waitFor(); 
	 }

	 
	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
						
		// TODO Auto-generated method stub
		String fname = "/home/xiao/Downloads/example.xml";
		String out_fname = "/home/xiao/Downloads/example_out.xml";
		
		ElevateManager ele = new ElevateManager();
		ele.readRules(fname);
		
		
		ArrayList<String> addedIds = new ArrayList<String>();
		ArrayList<String> deleteedIds = new ArrayList<String>();
		addedIds.add("123");
		addedIds.add("456");
		addedIds.add("789");
		
		deleteedIds.add("789");
		
		ele.addDocRule("a", "b", "c", "d", addedIds);
		ele.deleteDocRule("a", "b", "c", "d", deleteedIds);
		
		ArrayList<String> addedIds2 = new ArrayList<String>();
		addedIds2.add("123");
		addedIds2.add("456");
		addedIds2.add("789");
		addedIds2.add("1789");
		addedIds2.add("2789");
		ele.addDocRule("a", "b", "c", "d", addedIds2);
		
		addedIds2 = new ArrayList<String>();
		addedIds2.add("123");
		addedIds2.add("9456");		
		ele.addDocRule("a", "b", "c", "d", addedIds2);
		ele.addDocRule("a", "", "", "d", addedIds2);
		
		deleteedIds = new ArrayList<String>();
		deleteedIds.add("1789");
		deleteedIds.add("9456");
		ele.deleteDocRule("a", "b", "c", "d", deleteedIds);
		
		//ele.writeRules(out_fname);
		
		//ele.getHTML("http://localhost:8983/solr/admin/cores?action=RELOAD&core=query_bing_all_universities");
		
		ele.copy_file_remotely("/media/DataVolume1/apache-solr-3.4.0/example/solr/query_bing_all_universities/data/seeu_elevate.xml", "webserver@192.168.0.1", "/home/webserver/apache-solr-3.4.0/example/solr/query_bing_all_universities/data");
	}

}
