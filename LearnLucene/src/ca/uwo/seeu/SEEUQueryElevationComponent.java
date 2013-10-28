/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ca.uwo.seeu;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.StringReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.WeakHashMap;

import org.apache.solr.common.params.QueryElevationParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;
import org.apache.lucene.util.StringHelper;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.DOMUtil;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.core.Config;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.component.ResponseBuilder;
import org.apache.solr.handler.component.SearchComponent;
import org.apache.solr.schema.StrField;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.SortSpec;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.util.VersionedFile;
import org.apache.solr.util.RefCounted;
import org.apache.solr.util.plugin.SolrCoreAware;
import org.apache.solr.request.SolrQueryRequest;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

/**
 * A component to elevate some documents to the top of the result set.
 * 
 * @version $Id: QueryElevationComponent.java 1136465 2011-06-16 14:50:22 +0000 (Thu, 16 Jun 2011) mikemccand $
 * @since solr 1.3
 */
public class SEEUQueryElevationComponent extends SearchComponent implements SolrCoreAware
{
  private static Logger log = LoggerFactory.getLogger(SEEUQueryElevationComponent.class);
  
  // Constants used in solrconfig.xml
  static final String FIELD_TYPE = "queryFieldType";
  static final String CONFIG_FILE = "config-file";
  static final String EXCLUDE = "exclude";
  
  // Runtime param -- should be in common?

  private SolrParams initArgs = null;
  private Analyzer analyzer = null;
  private String idField = null;

  boolean forceElevation = false;
  // For each IndexReader, keep a query->elevation map
  // When the configuration is loaded from the data directory.
  // The key is null if loaded from the config directory, and
  // is never re-loaded.
  final Map<IndexReader,Map<String, ElevationObj>> elevationCache =
    new WeakHashMap<IndexReader, Map<String,ElevationObj>>();

  class ElevationObj {
    final String text;
    final String analyzed;
    final BooleanClause[] exclude;
    final BooleanQuery include;
    final Map<String,Integer> priority;
    
    // use singletons so hashCode/equals on Sort will just work
    final FieldComparatorSource comparatorSource;

    ElevationObj( String qstr, List<String> elevate, List<String> exclude ) throws IOException
    {
      this.text = qstr;
      this.analyzed = getAnalyzedQuery( this.text );
      
      this.include = new BooleanQuery();
      this.include.setBoost( 0 );
      this.priority = new HashMap<String, Integer>();
      /*
       * This is the key to elevate document
       * because other document does not have priority value. They
       * will be ranked lower.
       */
      int max = elevate.size()+5;
      for( String id : elevate ) {
        TermQuery tq = new TermQuery( new Term( idField, id ) );
        include.add( tq, BooleanClause.Occur.SHOULD );
        this.priority.put( id, max-- );
      }
      
      if( exclude == null || exclude.isEmpty() ) {
        this.exclude = null;
      }
      else {
        this.exclude = new BooleanClause[exclude.size()];
        for( int i=0; i<exclude.size(); i++ ) {
          TermQuery tq = new TermQuery( new Term( idField, exclude.get(i) ) );
          this.exclude[i] = new BooleanClause( tq, BooleanClause.Occur.MUST_NOT );
        }
      }

      this.comparatorSource = new ElevationComparatorSource(priority);
    }
  }
  
  @Override
  public void init( NamedList args )
  {
    this.initArgs = SolrParams.toSolrParams( args );
  }
  
  public void inform(SolrCore core)
  {
    String a = initArgs.get( FIELD_TYPE );
    if( a != null ) {
      FieldType ft = core.getSchema().getFieldTypes().get( a );
      if( ft == null ) {
        throw new SolrException( SolrException.ErrorCode.SERVER_ERROR,
            "Unknown FieldType: '"+a+"' used in QueryElevationComponent" );
      }
      analyzer = ft.getQueryAnalyzer();
    }

    SchemaField sf = core.getSchema().getUniqueKeyField();
    if( sf == null || !(sf.getType() instanceof StrField)) {
      throw new SolrException( SolrException.ErrorCode.SERVER_ERROR, 
          "QueryElevationComponent requires the schema to have a uniqueKeyField implemented using StrField" );
    }
    idField = StringHelper.intern(sf.getName());
    
    forceElevation = initArgs.getBool( QueryElevationParams.FORCE_ELEVATION, forceElevation );
    try {
      synchronized( elevationCache ) {
        elevationCache.clear();
        String f = initArgs.get( CONFIG_FILE );
        if( f == null ) {
          throw new SolrException( SolrException.ErrorCode.SERVER_ERROR,
              "QueryElevationComponent must specify argument: '"+CONFIG_FILE
              +"' -- path to elevate.xml" );
        }
        File fC = new File( core.getResourceLoader().getConfigDir(), f );
        File fD = new File( core.getDataDir(), f );
        if( fC.exists() == fD.exists() ) {
          throw new SolrException( SolrException.ErrorCode.SERVER_ERROR,
              "QueryElevationComponent missing config file: '"+f + "\n"
              +"either: "+fC.getAbsolutePath() + " or " + fD.getAbsolutePath() + " must exist, but not both." );
        }
        if( fC.exists() ) {
          log.info( "Loading QueryElevation from: "+fC.getAbsolutePath() );
          Config cfg = new Config( core.getResourceLoader(), f );
          elevationCache.put(null, loadElevationMap( cfg ));
        }
        else {
          // preload the first data
          RefCounted<SolrIndexSearcher> searchHolder = null;
          try {
            searchHolder = core.getNewestSearcher(false);
            IndexReader reader = searchHolder.get().getReader();
            getElevationMap( reader, core );
          } finally {
            if (searchHolder != null) searchHolder.decref();
          }
        }
      }
    }
    catch( Exception ex ) {
      throw new SolrException( SolrException.ErrorCode.SERVER_ERROR,
          "Error initializing QueryElevationComponent.", ex );
    }
  }

  Map<String, ElevationObj> getElevationMap( IndexReader reader, SolrCore core ) throws Exception
  {
    synchronized( elevationCache ) {
      Map<String, ElevationObj> map = elevationCache.get( null );
      if (map != null) return map;

      map = elevationCache.get( reader );
      if( map == null ) {
        String f = initArgs.get( CONFIG_FILE );
        if( f == null ) {
          throw new SolrException( SolrException.ErrorCode.SERVER_ERROR,
                  "QueryElevationComponent must specify argument: "+CONFIG_FILE );
        }
        log.info( "Loading QueryElevation from data dir: "+f );

        InputStream is = VersionedFile.getLatestFile( core.getDataDir(), f );
        Config cfg = new Config( core.getResourceLoader(), f, new InputSource(is), null );
        map = loadElevationMap( cfg );
        elevationCache.put( reader, map );
      }
      return map;
    }
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
  
  private Map<String, ElevationObj> loadElevationMap( Config cfg ) throws IOException
  {
    XPath xpath = XPathFactory.newInstance().newXPath();
    Map<String, ElevationObj> map = new HashMap<String, ElevationObj>();
    NodeList nodes = (NodeList)cfg.evaluate( "elevate/query", XPathConstants.NODESET );
    for (int i=0; i<nodes.getLength(); i++) {
      Node node = nodes.item( i );
      String qstr = DOMUtil.getAttr( node, "text", "missing query 'text'" );
      
      //add for category query      
      String catstr = DOMUtil.getAttr( node, "cat", "missing categories 'cat'" );
      catstr = string_to_sorted(catstr, " ", " ");
      
      //add for uname query      
      String unamestr = DOMUtil.getAttr( node, "uname", "missing university names 'uname'" );
      unamestr = string_to_sorted(unamestr, " ", " ");
      
      //add for file query      
      String filestr = DOMUtil.getAttr( node, "file", "missing file types 'file'" );
      filestr = string_to_sorted(filestr, " ", " ");
      
      NodeList children = null;
      try {
        children = (NodeList)xpath.evaluate("doc", node, XPathConstants.NODESET);
      } 
      catch (XPathExpressionException e) {
        throw new SolrException( SolrException.ErrorCode.SERVER_ERROR, 
            "query requires '<doc .../>' child" );
      }

      ArrayList<String> include = new ArrayList<String>();
      ArrayList<String> exclude = new ArrayList<String>();
      for (int j=0; j<children.getLength(); j++) {
        Node child = children.item(j);
        String id = DOMUtil.getAttr( child, "id", "missing 'id'" );
        String e = DOMUtil.getAttr( child, EXCLUDE, null );
        if( e != null ) {
          if( Boolean.valueOf( e ) ) {
            exclude.add( id );
            continue;
          }
        }
        include.add( id );
      }
      
      ElevationObj elev = new ElevationObj( qstr, include, exclude );
      if( map.containsKey( elev.analyzed ) ) {
        throw new SolrException( SolrException.ErrorCode.SERVER_ERROR, 
            "Boosting query defined twice for query: '"+elev.text+"' ("+elev.analyzed+"')" );
      }
      
      //aded by xiao 22/01/2013
      //the key is query and the category, uname, and file selection
      log.info("SEEU add rule " + elev.analyzed + "<<>>" + catstr + "<<>>" + unamestr + "<<>>" + filestr);
      map.put( elev.analyzed + "<<>>" + catstr + "<<>>" + unamestr + "<<>>" + filestr, elev );
    }
    return map;
  }
  
  /**
   * Helpful for testing without loading config.xml
   * @throws IOException 
   */
  void setTopQueryResults( IndexReader reader, String query, String[] ids, String[] ex ) throws IOException
  {
    if( ids == null ) {
      ids = new String[0];
    }
    if( ex == null ) {
      ex = new String[0];
    }
    
    Map<String,ElevationObj> elev = elevationCache.get( reader );
    if( elev == null ) {
      elev = new HashMap<String, ElevationObj>();
      elevationCache.put( reader, elev );
    }
    ElevationObj obj = new ElevationObj( query, Arrays.asList(ids), Arrays.asList(ex) );
    elev.put( obj.analyzed, obj );
  }
  
  String getAnalyzedQuery( String query ) throws IOException
  {
    if( analyzer == null ) {
      return query;
    }
    StringBuilder norm = new StringBuilder();
    TokenStream tokens = analyzer.reusableTokenStream( "", new StringReader( query ) );
    tokens.reset();
    
    CharTermAttribute termAtt = tokens.addAttribute(CharTermAttribute.class);
    while( tokens.incrementToken() ) {
      norm.append( termAtt.buffer(), 0, termAtt.length() );
    }
    return norm.toString();
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
  
  //---------------------------------------------------------------------------------
  // SearchComponent
  //---------------------------------------------------------------------------------
  
  @Override
  public void prepare(ResponseBuilder rb) throws IOException
  {
    SolrQueryRequest req = rb.req;
    SolrParams params = req.getParams();
    // A runtime param can skip 
    if( !params.getBool( QueryElevationParams.ENABLE, true ) ) {
      return;
    }

    boolean exclusive = params.getBool(QueryElevationParams.EXCLUSIVE, false);
    // A runtime parameter can alter the config value for forceElevation
    boolean force = params.getBool( QueryElevationParams.FORCE_ELEVATION, forceElevation );
    
    Query query = rb.getQuery();
    String qstr = rb.getQueryString();
    if( query == null || qstr == null) {
      return;
    }

    //get filters, this is very important
    //we will extract cat, uname and files from the filters
    List<Query> filters = rb.getFilters();
    
    qstr = getAnalyzedQuery(qstr);
    IndexReader reader = req.getSearcher().getReader();
    ElevationObj booster = null;
    
    log.info( "SEEU Getting Query: "+qstr + "   <>  " + query.toString());

    //extract the category, unames and files filters
    ArrayList<String> all_cat_filters = new ArrayList<String>(); 
    String cat_filter = "";
    String uname_filter = "";
    String file_filter = "";
    if(filters != null)
    {
	    for(int i=0;i<filters.size();i++)
	    {
	    	Query ft = filters.get(i);
	    	String str_ft = ft.toString();
	    	
	    	//get category
	    	int p = str_ft.indexOf(":");
	    	String str_ft_name = str_ft.substring(0, p);		
	    	if(str_ft_name.startsWith("cat_prob_"))
	    	{
	    		all_cat_filters.add(str_ft_name);
	    		log.info( "SEEU Getting QueryElevation category filters: "+ft.toString() + " " + str_ft_name);
	    	}
	    	
	    	//get unames
	    	if(str_ft.startsWith("uname:"))	    	
	    	{
	    		//format INFO: 
	    		//uname:uwaterloo uname:uwestern
	    		String uname_str = str_ft.replace("uname:", "");	    		
	    		uname_filter = this.string_to_sorted(uname_str, " ", " ");	    		
	    		log.info( "SEEU Getting QueryElevation uname filters: "+ft.toString() + " " + uname_filter);
	    	}
	    		
	    	//get file types
	    	if(str_ft.startsWith("contentType:"))
	    	{
	    		//format INFO: 
	    		//contentType:doc contentType:html contentType:pdf contentType:txt contentType:xml contentType:xls
	    		String file_str = str_ft.replace("contentType:", "");	    		
	    		file_filter = this.string_to_sorted(file_str, " ", " ");	    		
	    		log.info( "SEEU Getting QueryElevation file filters: "+ft.toString() + " " + file_filter);
	    	}
	    	
	    	log.info( "SEEU Getting filter " + str_ft);
	    }
    }
    
    //make filnal cat_filter
    if(all_cat_filters.size() != 0)
    	Collections.sort(all_cat_filters);
    cat_filter = join(all_cat_filters, " ");
    
    //query map table to get the rule
    try {//experiment to make fq:cat_prob_1181 works here
    	String rule_key = qstr + "<<>>" + cat_filter + "<<>>" + uname_filter + "<<>>" + file_filter;
    	log.info("SEEU check exception rule " + rule_key);
        booster = getElevationMap( reader, req.getCore() ).get( rule_key );
    }
    catch( Exception ex ) {
      throw new SolrException( SolrException.ErrorCode.SERVER_ERROR,
          "Error loading elevation", ex );      
    }
    
    //iteratate and print all rule key  
    /*
    Map<String, ElevationObj> allv = null;
	try {
		allv = getElevationMap(reader, req.getCore() );
	} catch (Exception e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
    Set<String> sit = allv.keySet();
    Iterator<String> it = sit.iterator();
    while(it.hasNext())
    {
    	log.info("SEEU all rule " + it.next());
    }
    */
    
    if( booster != null ) {
    	
    	log.info("SEEU rue found!");
    	
      // Change the query to insert forced documents
      if (exclusive == true){
        //we only want these results
        rb.setQuery(booster.include);
        
        log.info("SEEU new query" + booster.include.toString());
      } else {
        BooleanQuery newq = new BooleanQuery( true );
        newq.add( query, BooleanClause.Occur.SHOULD );
        newq.add( booster.include, BooleanClause.Occur.SHOULD );
        if( booster.exclude != null ) {
          for( BooleanClause bq : booster.exclude ) {
            newq.add( bq );
          }
        }
                	       
    	//change each filter query to add an OR constrain
        //for each filter query, it is changed to filter query OR id query
        if(filters != null)
        {
	    	for(int i=0;i<filters.size();i++)
	    	{        		
	    		Query q = filters.get(i);
	    		
	    		//make a new boolean query   
	    		BooleanQuery newfilter = new BooleanQuery( true );
	    		//add category filter    		
	    		newfilter.add(q, BooleanClause.Occur.SHOULD);    		
	    		//add id filter
	    		newfilter.add(booster.include, BooleanClause.Occur.SHOULD);
	
	    		//important!!
	    		//replace the old query with new query (cat OR id)
	    		filters.set(i, newfilter);
	    		
	    		log.info("SEEU change cat filter " + newfilter.toString());
	    	}
        }
            
        rb.setQuery( newq );
       //the filter is already changed
        
        log.info("SEEU new query" + newq.toString());
      }      
      
      // if the sort is 'score desc' use a custom sorting method to 
      // insert documents in their proper place 
      SortSpec sortSpec = rb.getSortSpec();
      if( sortSpec.getSort() == null ) {
        sortSpec.setSort( new Sort( new SortField[] {
            new SortField(idField, booster.comparatorSource, false ),
            new SortField(null, SortField.SCORE, false)
        }));
      }
      else {
        // Check if the sort is based on score
        boolean modify = false;
        SortField[] current = sortSpec.getSort().getSort();
        ArrayList<SortField> sorts = new ArrayList<SortField>( current.length + 1 );
        // Perhaps force it to always sort by score
        if( force && current[0].getType() != SortField.SCORE ) {
          sorts.add( new SortField(idField, booster.comparatorSource, false ) );
          modify = true;
        }
        for( SortField sf : current ) {
          if( sf.getType() == SortField.SCORE ) {
            sorts.add( new SortField(idField, booster.comparatorSource, sf.getReverse() ) );
            modify = true;
          }
          sorts.add( sf );
        }
        if( modify ) {
          sortSpec.setSort( new Sort( sorts.toArray( new SortField[sorts.size()] ) ) );
        }
      }
    }
    //pw.flush();
   // pw.close();
    // Add debugging information
    if( rb.isDebug() ) {
      List<String> match = null;
      if( booster != null ) {
        // Extract the elevated terms into a list
        match = new ArrayList<String>(booster.priority.size());
        for( Object o : booster.include.clauses() ) {
          TermQuery tq = (TermQuery)((BooleanClause)o).getQuery();
          match.add( tq.getTerm().text() );
        }
      }
      
      SimpleOrderedMap<Object> dbg = new SimpleOrderedMap<Object>();
      dbg.add( "q", qstr );
      dbg.add( "match", match );
      rb.addDebugInfo( "queryBoosting", dbg );
    }
  }

  @Override
  public void process(ResponseBuilder rb) throws IOException {
    // Do nothing -- the real work is modifying the input query
  }
    
  //---------------------------------------------------------------------------------
  // SolrInfoMBean
  //---------------------------------------------------------------------------------

  @Override
  public String getDescription() {
    return "Query Boosting -- boost particular documents for a given query";
  }

  @Override
  public String getVersion() {
    return "$Revision: 1136465 $";
  }

  @Override
  public String getSourceId() {
    return "$Id: QueryElevationComponent.java 1136465 2011-06-16 14:50:22 +0000 (Thu, 16 Jun 2011) mikemccand $";
  }

  @Override
  public String getSource() {
    return "$URL: https://svn.apache.org/repos/asf/lucene/dev/branches/lucene_solr_3_4/solr/core/src/java/org/apache/solr/handler/component/QueryElevationComponent.java $";
  }

  @Override
  public URL[] getDocs() {
    try {
      return new URL[] {
        new URL("http://wiki.apache.org/solr/QueryElevationComponent")
      };
    } 
    catch (MalformedURLException e) {
      throw new RuntimeException( e );
    }
  }
}

class ElevationComparatorSource extends FieldComparatorSource {
  private final Map<String,Integer> priority;

  public ElevationComparatorSource( final Map<String,Integer> boosts) {
    this.priority = boosts;
  }

  @Override
  public FieldComparator<Integer> newComparator(final String fieldname, final int numHits, int sortPos, boolean reversed) throws IOException {
    return new FieldComparator<Integer>() {
      
      FieldCache.StringIndex idIndex;
      private final int[] values = new int[numHits];
      int bottomVal;

      @Override
      public int compare(int slot1, int slot2) {
        return values[slot2] - values[slot1];  // values will be small enough that there is no overflow concern
      }

      @Override
      public void setBottom(int slot) {
        bottomVal = values[slot];
      }

      private int docVal(int doc) throws IOException {
        String id = idIndex.lookup[idIndex.order[doc]];
        Integer prio = priority.get(id);
        return prio == null ? 0 : prio.intValue();
      }

      @Override
      public int compareBottom(int doc) throws IOException {
        return docVal(doc) - bottomVal;
      }

      @Override
      public void copy(int slot, int doc) throws IOException {
        values[slot] = docVal(doc);
      }

      @Override
      public void setNextReader(IndexReader reader, int docBase) throws IOException {
        idIndex = FieldCache.DEFAULT.getStringIndex(reader, fieldname);
      }

      @Override
      public Integer value(int slot) {
        return values[slot];
      }
    };
  }
}
