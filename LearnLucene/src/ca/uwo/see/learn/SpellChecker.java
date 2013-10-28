package ca.uwo.see.learn;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermDocs;
import org.apache.lucene.index.TermEnum;
import org.apache.lucene.index.TermFreqVector;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.FSDirectory;

public class SpellChecker {

	private String indexDir = "/media/DataVolume1/apache-solr-3.4.0/example/solr/query_bing_uwestern/data/index";
	private String term_field = "content_no_stemming";	
	
	//generate candidates
	private CandidateGenerator cg = new CandidateGenerator();
			
	private HashMap<Integer, String> candidates = new HashMap<Integer, String>();
	
	private IndexReader ireader = null;
	private int threshold = 10;
	
	public void init()  throws IOException
	{
		//read solr index		
		try {
	        FSDirectory luceneIndexDir = FSDirectory.open(new File(indexDir));
	        ireader = IndexReader.open(luceneIndexDir);
	      } catch (IOException e) {
	        throw new RuntimeException(e);
	      }
		
		System.out.println(ireader.maxDoc() + " " + ireader.numDocs());
	}		
	
	public int getTermDocFreq(String q_text) throws IOException
	{				
		Term t = new Term(term_field, q_text);		
		TermEnum tm = ireader.terms(t);
		
		t = tm.term();
		String term_text = t.text();				
		if(term_text.startsWith(q_text))
		{				
			return tm.docFreq();
		}
		else
			return 0;		
	}
	
	public boolean isRareWord(String word) throws IOException
	{
		int docFreq = getTermDocFreq(word);		
		return docFreq < threshold;
	}
	
	
	public String correct(String q_text) throws IOException
	{
		candidates.clear();
		
		int maxDocFreq = 0;
		
		String correct_word = "";
		if(isRareWord(q_text) == false)
			correct_word = q_text;
		else
		{
			//edit distance is one
			ArrayList<String> list = cg.edits(q_text);
			for(String s : list) 
				if(isRareWord(s) == false) 			
					candidates.put(getTermDocFreq(s),s);
			correct_word = "";
			if(candidates.size() > 0) 
			{
				maxDocFreq = Collections.max(candidates.keySet());
				correct_word =  candidates.get(maxDocFreq);
			}
			else
			{
				//edit distance is two
				for(String s : list) 
				{
					ArrayList<String> editdis2 = cg.edits(s); 	
					//for(int j=0;j<editdis2.size();j++)
					//	System.out.println(q_text + " " + editdis2.get(j));
					for(String w : editdis2) 
						if(isRareWord(w) == false) 			
							candidates.put(getTermDocFreq(w),w);
				}
				if(candidates.size() > 0) 
				{
					maxDocFreq = Collections.max(candidates.keySet());
					correct_word  = candidates.size() > 0 ? candidates.get(maxDocFreq) : q_text;
				}
				else
				{
					correct_word = "";
				}								
			}
		}
		System.out.println(q_text + " " + candidates.size());
		return correct_word;
	}
	
	public HashMap<String, ArrayList<String>> readDataset(String fname) throws IOException
	{
		HashMap<String, ArrayList<String>> h = new HashMap<String, ArrayList<String>>();
		BufferedReader br = new BufferedReader(new FileReader(fname));
		String line = "";
		while((line = br.readLine()) != null)
		{
			String [] lines = line.split("<<>>");
			String correct = lines[0];
			String wrong = lines[1];
			
			ArrayList<String> wrongs = new ArrayList<String>();
			String [] str_wrongs = wrong.split(" ");
			for(int i=0;i<str_wrongs.length;i++)
				wrongs.add(str_wrongs[i]);
			
			h.put(correct, wrongs);
		}
		return h;		
	}
	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub		
		
		 
		String fname = "/home/xiao/test_spell_1.txt";
		
		
		String q_text = "systm";
		
		SpellChecker sc = new SpellChecker();
		
		//init index reader
		sc.init();
		
		int tot_word = 0;
		int tot_wrong = 0;
		
		HashMap<String, ArrayList<String>> dataset = sc.readDataset(fname);
		Set<String> e = dataset.keySet();		
		Iterator<String> it = e.iterator();
				
		long start = System.currentTimeMillis(); 
		while(it.hasNext())
		{
			String correct = it.next();
			ArrayList<String> wrongs = dataset.get(correct);
			for(int i=0;i<wrongs.size();i++)
			{
				String wrong = wrongs.get(i);
				long start_word = System.currentTimeMillis();
				String rem_word = sc.correct(wrong);
				long end_word = System.currentTimeMillis();
				
				tot_word++;
				if(rem_word.equalsIgnoreCase(correct) == false)
				{
					tot_wrong++;
					System.out.println(wrong + " " + rem_word + " " + correct + " run " + (end_word - start_word) + " mm seconds");
				}
				else
					System.out.println(wrong + " correct" + " run " + (end_word - start_word) + " mm  seconds");
			}					
		}
		long end = System.currentTimeMillis();
		System.out.println("wrong " + tot_wrong + " total " + tot_word + " " + (double)tot_wrong/tot_word);
		System.out.println("run " + (end - start) + " mm  seconds");
	}

}
