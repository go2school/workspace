package ca.uwo.see.learn;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Set;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermDocs;
import org.apache.lucene.index.TermEnum;
import org.apache.lucene.index.TermFreqVector;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.FSDirectory;

public class BuildLuceneDictionary {

	public String indexDir = "/media/DataVolume1/apache-solr-3.4.0/example/solr/query_bing_uwestern/data/index";
	private String term_field = "content_no_stemming";	
	
	//generate candidates
	private CandidateGenerator cg = new CandidateGenerator();
			
	private Hashtable<Integer, String> candidates = new Hashtable<Integer, String>();
	
	private IndexReader ireader = null;
	private int threshold = 10;
	private int word_length = 30;
	private Hashtable<String, dictionaryItem> dictionary = new Hashtable<String, dictionaryItem>();
	private int maxEditDistance = 2;
	
	private Hashtable<String, Integer> dictionaryCount = new Hashtable<String, Integer>();
	
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
	
	
	public boolean addEntryToDictionary(String key)
	{
		boolean result = false;		
		dictionaryItem v = dictionary.get(key);
		if(v == null)
		{
			v = new dictionaryItem();
			v.count ++;
			dictionary.put(key, v);
		}
		else
		{
			v.count++;
		}
		
		if(v.term == null || v.term.length() == 0)
		{
			result = true;
			v.term = key;
			//make deletion operatino on words with edit distance as maxeditdistance
			ArrayList<editItem> list = cg.editsRec(key, 0, maxEditDistance, true);
			for(int i=0;i<list.size();i++)
			{
				editItem suggestion = new editItem();
				suggestion.term = key;
				suggestion.distance = list.get(i).distance;
				
				dictionaryItem v2 = dictionary.get(list.get(i).term);
				if(v2 != null)
				{
					if(!v2.suggestions.contains(suggestion))
					{
						addLowestDistance(v2.suggestions, suggestion);
					}
				}
				else
				{
					v2 = new dictionaryItem();
					v2.suggestions.add(suggestion);
					dictionary.put(list.get(i).term, v2);
				}
			}
		}
		return result;
	}
	
	public void addLowestDistance(ArrayList<editItem> suggestions, editItem suggestion)
	{
		if(suggestions.size() > 0 || suggestions.get(0).distance > suggestion.distance) 
			suggestions.clear();
		if(suggestions.size() == 0 || suggestions.get(0).distance >= suggestion.distance)
			suggestions.add(suggestion);
	}
	
	public boolean isWord(String input)
	{
		boolean result = true;
		input = input.toLowerCase();
		char [] data = input.toCharArray();
		for(int i=0;i<input.length();i++)
			if(data[i] < 'a' || data[i] > 'z')
			{
				result = false;
				break;
			}
		return result;
	}
	
	public void buildDictionary() throws IOException
	{
		TermEnum tm = ireader.terms();
		int i=0, max = 10;
		while(tm.next())
		{			
			Term word = tm.term();
			if(isWord(word.text()) && word.text().length() < word_length && dictionaryCount.containsKey(word.text()) == false)
			{
				dictionaryCount.put(word.text(), getTermDocFreq(word.text()));
				i++;				
				if(i % 10000 == 0)
					System.out.println(i + " ..." + word.text());
			}	
		}
	}
	
	public void printDictionary(String fname) throws IOException
	{
		PrintWriter pw = new PrintWriter(new FileWriter(fname));
		Enumeration<String> e = dictionaryCount.keys();
		while(e.hasMoreElements())
		{
			String k = e.nextElement();			
			pw.print(k+" "+dictionaryCount.get(k) + "\n");
		}
		pw.flush();
		pw.close();
	}
	
	public float trueDistance(editItem dictionaryOriginal, editItem inputDelete, String inputOriginal)
	{
		if(dictionaryOriginal.term == inputOriginal) return 0;
		else if(dictionaryOriginal.distance == 0) return inputDelete.distance;
		else if(inputDelete.distance == 0) return dictionaryOriginal.distance;
		else return cg.getDistance(dictionaryOriginal.term, inputOriginal);
	}
	
	public class CustomCamparator implements Comparator<suggestItem>
	{

		@Override
		public int compare(suggestItem arg0, suggestItem arg1) {
			if(arg0.distance < arg1.distance)
				return -1;
			else if(arg0.distance == arg1.distance)
			{
				if(arg0.count < arg1.count)
					return -1;
				else if(arg0.count == arg1.count)
					return 0;
				else
					return 1;
			}
			else
				return 1;			
		}		
	}
	
	public ArrayList<suggestItem> correct(String input, int editDistanceMax)
	{
		ArrayList<editItem> candidates = new ArrayList<editItem>();
		ArrayList<suggestItem> suggestions = new ArrayList<suggestItem>();
		
		//add original term
		editItem item = new editItem();
		item.term = input;
		item.distance = 0;
		candidates.add(item);
		
		dictionaryItem value;
		
		while(candidates.size() > 0)
		{
			editItem candidate = candidates.get(0);
			candidates.remove(0);
			
			if(suggestions.size() > 0 && candidate.distance > suggestions.get(0).distance) break;
			
			if(candidate.distance > editDistanceMax) break;
			
			dictionaryItem v = dictionary.get(candidate.term);
			if(v != null)
			{
				if(v.term != null && v.term.length() != 0)
				{
					suggestItem si = new suggestItem();
					si.term = v.term;
					si.count = v.count;
					si.distance = candidate.distance;
					
					if(!suggestions.contains(si))
					{
						suggestions.add(si);
						if(candidate.distance == 0) break;
					}
				}
				
				dictionaryItem v2 = new dictionaryItem();
				
				for(int j=0;j<v.suggestions.size();j++)
				{
					editItem suggestion = v.suggestions.get(j); 
					//if()
					{
						float distance= trueDistance(suggestion, candidate, input);
						if(suggestions.size() > 0 && suggestions.get(0).distance > distance) suggestions.clear();
						if(suggestions.size() > 0 && distance > suggestions.get(0).distance) continue;
						
						if(distance <= editDistanceMax)
						{
							dictionaryItem v3 = dictionary.get(suggestion.term);
							if(v3 != null)
							{
								suggestItem si = new suggestItem();
								si.term = v3.term;
								si.count = v3.count;
								si.distance = distance;
								
								suggestions.add(si);
							}
						}
					}
				}
			}
			
			if(candidate.distance < editDistanceMax)
			{
				ArrayList<editItem> delete = cg.editsRec(candidate.term, candidate.distance, editDistanceMax, false);
				for(int k=0;k<delete.size();k++)
				{
					if(!candidates.contains(delete.get(k)))
						candidates.add(delete.get(k));
				}
			}
		}
		
		//sort
		Collections.sort(suggestions, new CustomCamparator());
		return suggestions;
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
		
		 
		
		String dict_fname = "/home/xiao/workspace/LearnLucene/data/dict.txt";
		
		
		BuildLuceneDictionary sc = new BuildLuceneDictionary();
		
		sc.indexDir = "/media/DataVolume1/apache-solr-3.4.0/example/solr/query_bing_all_universities/data/index";
		dict_fname = "/media/DataVolume1/apache-solr-3.4.0/example/solr/query_bing_all_universities/data/dict.txt";
		
		//init index reader
		sc.init();
		
		System.out.println("Init dictionary done");
		
		//build dictionary
		sc.buildDictionary();
		
		sc.printDictionary(dict_fname);
				
		System.out.println("Build dictionary done");		
	}

}
