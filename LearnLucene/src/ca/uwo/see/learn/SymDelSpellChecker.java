package ca.uwo.see.learn;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Set;

import org.apache.commons.codec.language.DoubleMetaphone;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermDocs;
import org.apache.lucene.index.TermEnum;
import org.apache.lucene.index.TermFreqVector;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.FSDirectory;

public class SymDelSpellChecker {

	private String indexDir = "/media/DataVolume1/apache-solr-3.4.0/example/solr/query_bing_uwestern/data/index";
	private String term_field = "content_no_stemming";

	// generate candidates
	private CandidateGenerator cg = new CandidateGenerator();

	private Hashtable<Integer, String> candidates = new Hashtable<Integer, String>();

	private IndexReader ireader = null;
	private int threshold = 10;

	private Hashtable<String, dictionaryItem> dictionary = new Hashtable<String, dictionaryItem>();

	// Doublemetaphone algorithm
	public DoubleMetaphone dmp = new DoubleMetaphone();

	// bayes parameter
	public static double alpha = 0.5;
	public static double beta = 1.0;

	public static int debug = 0;

	public static boolean isUseDoubleMetaPhone = false;// if rerank by metaphone
	public static boolean useRangeTest = true;// compute correctness within some
												// words
	public static int rangeTest = 3;// compute correctness within five words
	public static int max_dict_size = 50000;// get maximal 10000 dictionary size
	public static int occ_threshold = 2;// ignore words less than 2
	public static int length_threshold = 15;// ignore words longer than 10
	public static int maxEditDistance = 3;// use max edit distance as 3

	public void init() throws IOException {
		// read solr index
		try {
			FSDirectory luceneIndexDir = FSDirectory.open(new File(indexDir));
			ireader = IndexReader.open(luceneIndexDir);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}

		System.out.println(ireader.maxDoc() + " " + ireader.numDocs());
	}

	public int getTermDocFreq(String q_text) throws IOException {
		Term t = new Term(term_field, q_text);
		TermEnum tm = ireader.terms(t);

		t = tm.term();
		String term_text = t.text();
		if (term_text.startsWith(q_text)) {
			return tm.docFreq();
		} else
			return 0;
	}

	public boolean isRareWord(String word) throws IOException {
		int docFreq = getTermDocFreq(word);
		return docFreq < threshold;
	}

	public boolean addEntryToDictionary(String key, int docFreq) {
		boolean result = false;
		dictionaryItem v = dictionary.get(key);
		if (v == null) {
			v = new dictionaryItem();
			v.count += docFreq;
			dictionary.put(key, v);
		} else {
			v.count += docFreq;
		}

		if (v.term == null || v.term.length() == 0) {
			result = true;
			v.term = key;
			// make deletion operatino on words with edit distance as
			// maxeditdistance
			ArrayList<editItem> list = cg.editsRec(key, 0, maxEditDistance,
					true);
			for (int i = 0; i < list.size(); i++) {
				editItem suggestion = new editItem();
				suggestion.term = key;
				// suggestion.count += docFreq;
				suggestion.distance = list.get(i).distance;

				dictionaryItem v2 = dictionary.get(list.get(i).term);
				if (v2 != null) {
					if (!v2.suggestions.contains(suggestion)) {
						addLowestDistance(v2.suggestions, suggestion);
					}
				} else {
					v2 = new dictionaryItem();
					v2.suggestions.add(suggestion);
					dictionary.put(list.get(i).term, v2);
				}
			}
		}
		return result;
	}

	public void addLowestDistance(ArrayList<editItem> suggestions,
			editItem suggestion) {
		if (suggestions.size() > 0
				&& suggestions.get(0).distance > suggestion.distance)
			suggestions.clear();
		if (suggestions.size() == 0
				|| suggestions.get(0).distance >= suggestion.distance)
			suggestions.add(suggestion);
	}

	public boolean isWord(String input) {
		boolean result = true;
		input = input.toLowerCase();
		char[] data = input.toCharArray();
		for (int i = 0; i < input.length(); i++)
			if (data[i] < 'a' || data[i] > 'z') {
				result = false;
				break;
			}
		return result;
	}

	public void buildDictionary() throws IOException {
		TermEnum tm = ireader.terms();
		int i = 0, max = 10;
		while (tm.next()) {
			Term word = tm.term();
			if (isWord(word.text())) {
				// addEntryToDictionary(word.text());
				i++;
				if (i == max)
					break;
				if (i % 100 == 0)
					System.out.println(i + " ...");
			}
		}
	}

	public void buildDictionaryFromFile(String fname, int max_word,
			int occ_threshold, int length_threshold) throws IOException {
		BufferedReader br = new BufferedReader(new FileReader(fname));
		String line = "";
		int i = 0;
		while ((line = br.readLine()) != null) {
			String[] vs = line.split(" ");
			int count = Integer.parseInt(vs[1]);
			if (count >= occ_threshold && vs[0].length() <= length_threshold) {
				addEntryToDictionary(vs[0], count);
				i++;
				if (i == max_word)
					break;
				if (i % 1000 == 0)
					System.out.println(i + " ...");
			}
		}
		br.close();
	}

	public void printDictionary() {
		Enumeration<String> e = dictionary.keys();
		while (e.hasMoreElements()) {
			String k = e.nextElement();
			dictionaryItem di = dictionary.get(k);
			// System.out.println(k + "=>" + di.toString());
		}
	}

	public float trueDistance(editItem dictionaryOriginal,
			editItem inputDelete, String inputOriginal) {
		if (dictionaryOriginal.term == inputOriginal)
			return 0;
		else if (dictionaryOriginal.distance == 0)
			return inputDelete.distance;
		else if (inputDelete.distance == 0)
			return dictionaryOriginal.distance;
		else {
			// return cg.getDistance(dictionaryOriginal.term, inputOriginal);
			return cg.DamerauLevenshteinDistance(dictionaryOriginal.term,
					inputOriginal);
		}
	}

	public class CustomCamparator implements Comparator<suggestItem> {

		@Override
		public int compare(suggestItem arg0, suggestItem arg1) {
			if (arg0.distance < arg1.distance)
				return -1;
			else if (arg0.distance == arg1.distance) {
				if (arg0.count < arg1.count)
					return 1;
				else if (arg0.count == arg1.count)
					return 0;
				else
					return -1;
			} else
				return 1;
		}
	}

	public double bayesScore(suggestItem s) {
		double ret = Math.log(s.count)
				/ ((s.distance + 1) * (s.metaphone_distance + 1));
		// double ret = Math.log(s.count) / (s.distance * alpha +
		// s.metaphone_distance * (1 - alpha) + beta);
		return ret;
	}

	public class CustomCamparatorBayes implements Comparator<suggestItem> {

		@Override
		public int compare(suggestItem arg0, suggestItem arg1) {
			double a0 = bayesScore(arg0);
			double a1 = bayesScore(arg1);
			if (a0 < a1)
				return 1;
			else if (a0 == a1)
				return 0;
			else
				return -1;

		}
	}

	public boolean findSame(ArrayList<suggestItem> suggestions, String term) {
		if (suggestions.size() == 0)
			return false;
		boolean result = false;
		for (int i = 0; i < suggestions.size(); i++) {
			if (suggestions.get(i).term.equals(term)) {
				result = true;
				break;
			}
		}
		return result;
	}

	public ArrayList<suggestItem> correct(String input) {
		ArrayList<editItem> candidates = new ArrayList<editItem>();
		ArrayList<suggestItem> suggestions = new ArrayList<suggestItem>();

		// add original term
		editItem item = new editItem();
		item.term = input;
		item.distance = 0;
		candidates.add(item);

		// get input metaphone
		String input_dmp = dmp.doubleMetaphone(input);

		while (candidates.size() > 0) {
			editItem candidate = candidates.get(0);
			candidates.remove(0);

			// if(suggestions.size() > 0 && candidate.distance >
			// suggestions.get(0).distance)
			// break;

			if (candidate.distance > maxEditDistance)
				break;

			dictionaryItem v = dictionary.get(candidate.term);
			if (v != null) {
				if (v.term != null && v.term.length() != 0) {
					suggestItem si = new suggestItem();
					si.term = v.term;
					si.count = v.count;
					si.distance = candidate.distance;

					// compute metaphone
					String dmp_sug = dmp.doubleMetaphone(v.term);
					// compute meta phone distance
					si.metaphone_distance = cg.DamerauLevenshteinDistance(
							input_dmp, dmp_sug);
					if ((dmp_sug.length() == 0 && input_dmp.length() != 0)
							|| (dmp_sug.length() != 0 && input_dmp.length() == 0)
							|| (dmp_sug.length() != 0
									&& input_dmp.length() != 0 && dmp_sug
									.charAt(0) != input_dmp.charAt(0)))
						si.metaphone_distance += 0.5;// punish the first letter
														// difference
					if (!suggestions.contains(si)) {
						suggestions.add(si);
						// if(candidate.distance == 0)
						// break;
					}
				}

				for (int j = 0; j < v.suggestions.size(); j++) {
					editItem suggestion = v.suggestions.get(j);
					if (findSame(suggestions, suggestion.term) == false) {
						float distance = trueDistance(suggestion, candidate,
								input);
						/*
						 * if(suggestions.size() > 0 &&
						 * suggestions.get(0).distance > distance)
						 * suggestions.clear(); if(suggestions.size() > 0 &&
						 * distance > suggestions.get(0).distance) continue;
						 */

						if (distance <= maxEditDistance) {
							dictionaryItem v3 = dictionary.get(suggestion.term);
							if (v3 != null) {
								suggestItem si = new suggestItem();
								si.term = v3.term;
								si.count = v3.count;
								si.distance = distance;

								// compute metaphone
								String dmp_sug = dmp.doubleMetaphone(v3.term);
								// compute meta phone distance
								si.metaphone_distance = cg
										.DamerauLevenshteinDistance(input_dmp,
												dmp_sug);
								// check if first letter different
								if ((dmp_sug.length() == 0 && input_dmp
										.length() != 0)
										|| (dmp_sug.length() != 0 && input_dmp
												.length() == 0)
										|| (dmp_sug.length() != 0
												&& input_dmp.length() != 0 && dmp_sug
												.charAt(0) != input_dmp
												.charAt(0)))
									si.metaphone_distance += 0.5;// punish the
																	// first
																	// letter
																	// difference
								suggestions.add(si);
							}
						}
					}
				}
			}

			if (candidate.distance < maxEditDistance) {
				ArrayList<editItem> delete = cg.editsRec(candidate.term,
						candidate.distance, maxEditDistance, false);
				for (int k = 0; k < delete.size(); k++) {
					if (!candidates.contains(delete.get(k)))
						candidates.add(delete.get(k));
				}
			}
		}

		// sort
		// Collections.sort(suggestions, new CustomCamparator());
		Collections.sort(suggestions, new CustomCamparatorBayes());
		return suggestions;
	}

	public ArrayList<suggestItem> reRankByDoubleMetaPhone(String old,
			ArrayList<suggestItem> suggestions) {
		ArrayList<suggestItem> ret = new ArrayList<suggestItem>();

		// put the similar metaphone in front
		for (int i = 0; i < suggestions.size(); i++) {
			if (dmp.isDoubleMetaphoneEqual(old, suggestions.get(i).term)) {
				ret.add(suggestions.get(i));
			}
		}

		// leave not similar behind
		for (int i = 0; i < suggestions.size(); i++) {
			if (dmp.isDoubleMetaphoneEqual(old, suggestions.get(i).term) == false) {
				ret.add(suggestions.get(i));
			}
		}

		return ret;
	}

	public String correct2(String q_text) throws IOException {
		candidates.clear();

		int maxDocFreq = 0;

		String correct_word = "";
		if (isRareWord(q_text) == false)
			correct_word = q_text;
		else {
			// edit distance is one
			ArrayList<String> list = cg.edits(q_text);
			for (String s : list)
				if (isRareWord(s) == false)
					candidates.put(getTermDocFreq(s), s);
			correct_word = "";
			if (candidates.size() > 0) {
				maxDocFreq = Collections.max(candidates.keySet());
				correct_word = candidates.get(maxDocFreq);
			} else {
				// edit distance is two
				for (String s : list) {
					ArrayList<String> editdis2 = cg.edits(s);
					// for(int j=0;j<editdis2.size();j++)
					// System.out.println(q_text + " " + editdis2.get(j));
					for (String w : editdis2)
						if (isRareWord(w) == false)
							candidates.put(getTermDocFreq(w), w);
				}
				if (candidates.size() > 0) {
					maxDocFreq = Collections.max(candidates.keySet());
					correct_word = candidates.size() > 0 ? candidates
							.get(maxDocFreq) : q_text;
				} else {
					correct_word = "";
				}
			}
		}
		System.out.println(q_text + " " + candidates.size());
		return correct_word;
	}

	public HashMap<String, ArrayList<String>> readDataset(String fname)
			throws IOException {
		HashMap<String, ArrayList<String>> h = new HashMap<String, ArrayList<String>>();
		BufferedReader br = new BufferedReader(new FileReader(fname));
		String line = "";
		while ((line = br.readLine()) != null) {
			String[] lines = line.split("<<>>");
			String correct = lines[0];
			String wrong = lines[1];

			ArrayList<String> wrongs = new ArrayList<String>();
			String[] str_wrongs = wrong.split(" ");
			for (int i = 0; i < str_wrongs.length; i++)
				wrongs.add(str_wrongs[i]);

			h.put(correct, wrongs);
		}
		return h;
	}

	public HashMap<String, ArrayList<String>> readDatasetOTA(String fname)
			throws IOException {
		HashMap<String, ArrayList<String>> h = new HashMap<String, ArrayList<String>>();
		BufferedReader br = new BufferedReader(new FileReader(fname));
		String line = "";
		while ((line = br.readLine()) != null) {
			String wrong = "";
			String correct = "";

			if (fname.contains("FAWTHROP1DAT.643")
					|| fname.contains("SHEFFIELDDAT.643")) {
				String[] lines = line.split(" ");
				correct = lines[0].toLowerCase();
				wrong = lines[lines.length - 1].toLowerCase();
			} else {
				if (line.charAt(0) == '$')
					continue;

				String[] lines = line.split(" ");
				wrong = lines[0].toLowerCase();
				int j = 0;
				for (j = 0; j < lines.length; j++)
					if (lines[j] != "")
						break;
				correct = lines[j].toLowerCase();
				if (correct.charAt(0) == '(') {
					correct = correct.substring(1, correct.length() - 1);
				}
			}
			ArrayList<String> wrongs = new ArrayList<String>();
			wrongs.add(wrong);

			h.put(correct, wrongs);
		}
		return h;
	}

	public HashMap<String, ArrayList<String>> readDatasetOTA2(String fname)
			throws IOException {
		HashMap<String, ArrayList<String>> h = new HashMap<String, ArrayList<String>>();
		BufferedReader br = new BufferedReader(new FileReader(fname));
		String line = "";
		while ((line = br.readLine()) != null) {
			String correct = "";
			String[] lines = line.split(" ");
			correct = lines[0].toLowerCase();
			ArrayList<String> wrongs = new ArrayList<String>();
			for (int i = 1; i < lines.length; i++)
				wrongs.add(lines[i].toLowerCase());

			h.put(correct, wrongs);
		}
		return h;
	}

	/**
	 * @param args
	 * @throws IOException
	 */
	public static void testSpellChecker() throws IOException {
		// TODO Auto-generated method stub

		String fname = "/home/xiao/test_spell_1.txt";

		String dict_fname = "/home/xiao/workspace/LearnLucene/data/dict_count_sorted.txt";
		dict_fname = "/media/DataVolume1/apache-solr-3.4.0/example/solr/query_bing_all_universities/data/dict_count_sorted.txt";
		boolean isUseDoubleMetaPhone = false;// if rerank by metaphone
		boolean useRangeTest = true;// compute correctness within some words
		int rangeTest = 5;// compute correctness within five words
		int max_dict_size = 50000;// get maximal 10000 dictionary size
		int occ_threshold = 2;// ignore words less than 2
		int length_threshold = 15;// ignore words longer than 10
		int maxEditDistance = 3;// use max edit distance as 3

		SymDelSpellChecker sc = new SymDelSpellChecker();

		// init index reader
		sc.init();

		sc.maxEditDistance = maxEditDistance;

		System.out.println("Init dictionary done");

		// build dictionary
		// sc.buildDictionary();
		sc.buildDictionaryFromFile(dict_fname, max_dict_size, occ_threshold,
				length_threshold);

		sc.printDictionary();

		System.out.println("Build dictionary done");

		int tot_word = 0;
		int tot_wrong = 0;

		HashMap<String, ArrayList<String>> dataset = sc.readDataset(fname);
		Set<String> e = dataset.keySet();
		Iterator<String> it = e.iterator();

		long start = System.currentTimeMillis();
		while (it.hasNext()) {
			String correct = it.next();
			ArrayList<String> wrongs = dataset.get(correct);
			for (int i = 0; i < wrongs.size(); i++) {
				String wrong = wrongs.get(i);
				long start_word = System.currentTimeMillis();
				ArrayList<suggestItem> suggestions = sc.correct(wrong);

				if (isUseDoubleMetaPhone) {
					ArrayList<suggestItem> tmp = sc.reRankByDoubleMetaPhone(
							wrong, suggestions);
					suggestions = tmp;
				}

				String rem_word = "";
				int count = 0;
				if (suggestions.size() > 0) {
					rem_word = suggestions.get(0).term;
					count = suggestions.get(0).count;
				}
				long end_word = System.currentTimeMillis();

				tot_word++;

				boolean is_correct = rem_word.equalsIgnoreCase(correct);
				if (is_correct == false) {
					// check all again
					boolean right = false;
					if (useRangeTest) {
						for (int k = 0; k < suggestions.size() && k < rangeTest; k++) {
							if (suggestions.get(k).term
									.equalsIgnoreCase(correct) == true) {
								right = true;
								break;
							}
						}
					}
					if (right == false) {
						tot_wrong++;
						System.out.println("[wrong  ] " + wrong + " "
								+ sc.dmp.doubleMetaphone(wrong) + " "
								+ rem_word + ":" + count + " " + correct + " "
								+ sc.dmp.doubleMetaphone(correct) + " run "
								+ (end_word - start_word) + " mm seconds");
						for (int k = 0; k < suggestions.size()
								&& k < rangeTest * 2; k++) {
							if (suggestions.get(k).term.equals(correct))
								System.out.println("*** "
										+ suggestions.get(k).term
										+ " "
										+ sc.dmp.doubleMetaphone(suggestions
												.get(k).term) + " "
										+ suggestions.get(k).count + " "
										+ suggestions.get(k).distance + " "
										+ suggestions.get(k).metaphone_distance
										+ " "
										+ sc.bayesScore(suggestions.get(k)));
							else
								System.out.println("    "
										+ suggestions.get(k).term
										+ " "
										+ sc.dmp.doubleMetaphone(suggestions
												.get(k).term) + " "
										+ suggestions.get(k).count + " "
										+ suggestions.get(k).distance + " "
										+ suggestions.get(k).metaphone_distance
										+ " "
										+ sc.bayesScore(suggestions.get(k)));
						}
					}
				}
			}
		}
		long end = System.currentTimeMillis();
		System.out.println("wrong " + tot_wrong + " total " + tot_word + " "
				+ (double) tot_wrong / tot_word);
		System.out.println("run " + (end - start) + " mm  seconds");

	}

	public static void evaluate(SymDelSpellChecker sc, String fname)
			throws IOException {
		int tot_word = 0;
		int[] tot_corrects = new int[rangeTest];
		int tot_right = 0;

		HashMap<String, ArrayList<String>> dataset = sc.readDatasetOTA2(fname);
		Set<String> e = dataset.keySet();
		Iterator<String> it = e.iterator();

		long start = System.currentTimeMillis();
		while (it.hasNext()) {
			String correct = it.next();
			ArrayList<String> wrongs = dataset.get(correct);
			for (int i = 0; i < wrongs.size(); i++) {
				String wrong = wrongs.get(i);
				long start_word = System.currentTimeMillis();
				ArrayList<suggestItem> suggestions = sc.correct(wrong);

				if (isUseDoubleMetaPhone) {
					ArrayList<suggestItem> tmp = sc.reRankByDoubleMetaPhone(
							wrong, suggestions);
					suggestions = tmp;
				}

				String rem_word = "";
				int count = 0;
				if (suggestions.size() > 0) {
					rem_word = suggestions.get(0).term;
					count = suggestions.get(0).count;
				}
				long end_word = System.currentTimeMillis();

				tot_word++;

				boolean is_correct = rem_word.equalsIgnoreCase(correct);
				if (is_correct == false) {
					// check all again
					boolean right = false;
					if (useRangeTest) {
						for (int k = 0; k < suggestions.size() && k < rangeTest; k++) {
							if (suggestions.get(k).term
									.equalsIgnoreCase(correct) == true) {
								right = true;
								tot_corrects[k]++;
							}
						}
					}
					if (right == false) {
						if (debug >= 1) {
							System.out.println("[wrong  ] " + wrong + " "
									+ sc.dmp.doubleMetaphone(wrong) + " "
									+ rem_word + ":" + count + " " + correct
									+ " " + sc.dmp.doubleMetaphone(correct)
									+ " run " + (end_word - start_word)
									+ " mm seconds");
							for (int k = 0; k < suggestions.size()
									&& k < rangeTest * 2; k++) {
								if (suggestions.get(k).term.equals(correct))
									System.out
											.println("*** "
													+ suggestions.get(k).term
													+ " "
													+ sc.dmp.doubleMetaphone(suggestions
															.get(k).term)
													+ " "
													+ suggestions.get(k).count
													+ " "
													+ suggestions.get(k).distance
													+ " "
													+ suggestions.get(k).metaphone_distance
													+ " "
													+ sc.bayesScore(suggestions
															.get(k)));
								else
									System.out
											.println("    "
													+ suggestions.get(k).term
													+ " "
													+ sc.dmp.doubleMetaphone(suggestions
															.get(k).term)
													+ " "
													+ suggestions.get(k).count
													+ " "
													+ suggestions.get(k).distance
													+ " "
													+ suggestions.get(k).metaphone_distance
													+ " "
													+ sc.bayesScore(suggestions
															.get(k)));
							}
						}
					} else
						tot_right++;
				} else {
					tot_right++;
					tot_corrects[0]++;
				}
			}
		}
		long end = System.currentTimeMillis();
		System.out.println(fname.substring(fname.lastIndexOf('/')) + " acc "
				+ tot_right + "/" + tot_word + " ("
				+ String.format("%.3f", (double) tot_right / tot_word)
				+ ") run " + (end - start) + " mm seconds");
		if (useRangeTest) {
			for (int k = 0; k < rangeTest; k++) {
				if (k > 1)
					tot_corrects[k] += tot_corrects[k - 1];
				System.out.println("     "
						+ k
						+ " acc "
						+ tot_corrects[k]
						+ "/"
						+ tot_word
						+ " ("
						+ String.format("%.3f", (double) tot_corrects[k]
								/ tot_word) + ")");
			}
		}
	}

	public ArrayList<suggestItem> getCorrector(String wrong) throws IOException {
		ArrayList<suggestItem> suggestions = correct(wrong);

		if (isUseDoubleMetaPhone) {
			ArrayList<suggestItem> tmp = reRankByDoubleMetaPhone(wrong,
					suggestions);
			suggestions = tmp;
		}

		return suggestions;
	}

	public static SymDelSpellChecker learnSpellChecker(String dict_fname)
			throws IOException {

		SymDelSpellChecker sc = new SymDelSpellChecker();

		
		System.out.println("Init dictionary done");

		sc.buildDictionaryFromFile(dict_fname, max_dict_size, occ_threshold,
				length_threshold);

		// sc.printDictionary();

		System.out.println("Build dictionary done");

		return sc;
	}

	/**
	 * @param args
	 * @throws IOException
	 */
	public static void testSpellCheckerOTA() throws IOException {
		// TODO Auto-generated method stub

		String fname = "/media/DataVolume1/datasets/spelling/0643/FAWTHROP1DAT.643";

		String dict_fname = "/home/xiao/workspace/LearnLucene/data/dict_count_sorted.txt";
		dict_fname = "/media/DataVolume1/apache-solr-3.4.0/example/solr/query_bing_all_universities/data/dict_count_sorted.txt";

		SymDelSpellChecker sc = new SymDelSpellChecker();

		// init index reader
		sc.init();

		sc.maxEditDistance = maxEditDistance;

		System.out.println("Init dictionary done");

		// build dictionary
		// sc.buildDictionary();
		sc.buildDictionaryFromFile(dict_fname, max_dict_size, occ_threshold,
				length_threshold);

		sc.printDictionary();

		System.out.println("Build dictionary done");

		int tot_word = 0;
		int tot_wrong = 0;

		HashMap<String, ArrayList<String>> dataset = sc.readDatasetOTA(fname);
		Set<String> e = dataset.keySet();
		Iterator<String> it = e.iterator();

		long start = System.currentTimeMillis();
		while (it.hasNext()) {
			String correct = it.next();
			ArrayList<String> wrongs = dataset.get(correct);
			for (int i = 0; i < wrongs.size(); i++) {
				String wrong = wrongs.get(i);
				long start_word = System.currentTimeMillis();
				ArrayList<suggestItem> suggestions = sc.correct(wrong);

				if (isUseDoubleMetaPhone) {
					ArrayList<suggestItem> tmp = sc.reRankByDoubleMetaPhone(
							wrong, suggestions);
					suggestions = tmp;
				}

				String rem_word = "";
				int count = 0;
				if (suggestions.size() > 0) {
					rem_word = suggestions.get(0).term;
					count = suggestions.get(0).count;
				}
				long end_word = System.currentTimeMillis();

				tot_word++;

				boolean is_correct = rem_word.equalsIgnoreCase(correct);
				if (is_correct == false) {
					// check all again
					boolean right = false;
					if (useRangeTest) {
						for (int k = 0; k < suggestions.size() && k < rangeTest; k++) {
							if (suggestions.get(k).term
									.equalsIgnoreCase(correct) == true) {
								right = true;
								break;
							}
						}
					}
					if (right == false) {
						tot_wrong++;
						System.out.println("[wrong  ] " + wrong + " "
								+ sc.dmp.doubleMetaphone(wrong) + " "
								+ rem_word + ":" + count + " " + correct + " "
								+ sc.dmp.doubleMetaphone(correct) + " run "
								+ (end_word - start_word) + " mm seconds");
						for (int k = 0; k < suggestions.size()
								&& k < rangeTest * 2; k++) {
							if (suggestions.get(k).term.equals(correct))
								System.out.println("*** "
										+ suggestions.get(k).term
										+ " "
										+ sc.dmp.doubleMetaphone(suggestions
												.get(k).term) + " "
										+ suggestions.get(k).count + " "
										+ suggestions.get(k).distance + " "
										+ suggestions.get(k).metaphone_distance
										+ " "
										+ sc.bayesScore(suggestions.get(k)));
							else
								System.out.println("    "
										+ suggestions.get(k).term
										+ " "
										+ sc.dmp.doubleMetaphone(suggestions
												.get(k).term) + " "
										+ suggestions.get(k).count + " "
										+ suggestions.get(k).distance + " "
										+ suggestions.get(k).metaphone_distance
										+ " "
										+ sc.bayesScore(suggestions.get(k)));
						}
					}
				}
			}
		}
		long end = System.currentTimeMillis();
		System.out.println("wrong " + tot_wrong + " total " + tot_word + " "
				+ (double) tot_wrong / tot_word);
		System.out.println("run " + (end - start) + " mm  seconds");

	}

	public static void toy() throws IOException {
		String fname = "/home/xiao/test_spell_1.txt";

		String dict_fname = "/home/xiao/workspace/LearnLucene/data/dict_count_sorted.txt";
		dict_fname = "/media/DataVolume1/apache-solr-3.4.0/example/solr/query_bing_all_universities/data/dict_count_sorted.txt";
		boolean isUseDoubleMetaPhone = false;// if rerank by metaphone
		boolean useRangeTest = true;// compute correctness within some words
		int rangeTest = 5;// compute correctness within five words
		int max_dict_size = 50000;// get maximal 10000 dictionary size
		int occ_threshold = 2;// ignore words less than 2
		int length_threshold = 15;// ignore words longer than 10
		int maxEditDistance = 3;// use max edit distance as 3

		SymDelSpellChecker sc = new SymDelSpellChecker();

		// init index reader
		sc.init();

		sc.maxEditDistance = maxEditDistance;

		System.out.println("Init dictionary done");

		// build dictionary
		// sc.buildDictionary();
		sc.buildDictionaryFromFile(dict_fname, max_dict_size, occ_threshold,
				length_threshold);

		sc.printDictionary();

		System.out.println("Build dictionary done");

		int tot_word = 0;
		int tot_wrong = 0;

		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		String wrong = "";
		long start = System.currentTimeMillis();
		while (true) {
			wrong = br.readLine();
			if (wrong.equals("q"))
				break;
			long start_word = System.currentTimeMillis();
			ArrayList<suggestItem> suggestions = sc.correct(wrong);

			if (isUseDoubleMetaPhone) {
				ArrayList<suggestItem> tmp = sc.reRankByDoubleMetaPhone(wrong,
						suggestions);
				suggestions = tmp;
			}

			String rem_word = "";
			int count = 0;
			if (suggestions.size() > 0) {
				rem_word = suggestions.get(0).term;
				count = suggestions.get(0).count;
			}
			long end_word = System.currentTimeMillis();

			tot_word++;

			System.out
					.println("[wrong  ] " + wrong + " "
							+ sc.dmp.doubleMetaphone(wrong) + " " + rem_word
							+ ":" + count + " run " + (end_word - start_word)
							+ " mm seconds");
			for (int k = 0; k < suggestions.size() && k < 10; k++)
				System.out.println("*** " + suggestions.get(k).term + " "
						+ sc.dmp.doubleMetaphone(suggestions.get(k).term) + " "
						+ suggestions.get(k).count + " "
						+ suggestions.get(k).distance + " "
						+ suggestions.get(k).metaphone_distance + " "
						+ sc.bayesScore(suggestions.get(k)));
		}

		long end = System.currentTimeMillis();

		System.out.println("wrong " + tot_wrong + " total " + tot_word + " "
				+ (double) tot_wrong / tot_word);
		System.out.println("run " + (end - start) + " mm  seconds");
	}

	/**
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		// testSpellChecker();

		SymDelSpellChecker sc = learnSpellChecker("/media/DataVolume1/apache-solr-3.4.0/example/solr/query_bing_all_universities/data/dict_count_sorted.txt");

		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		String wrong = "";
		long start = System.currentTimeMillis();
		while (true) {
			wrong = br.readLine();
			if (wrong.equals("q"))
				break;
			long start_word = System.currentTimeMillis();
			ArrayList<suggestItem> suggestions = sc.correct(wrong);

			if (isUseDoubleMetaPhone) {
				ArrayList<suggestItem> tmp = sc.reRankByDoubleMetaPhone(wrong,
						suggestions);
				suggestions = tmp;
			}

			for (int i = 0; i < 5 && i < suggestions.size(); i++) {
				System.out.println(suggestions.get(i).term);
			}
		}
		/*
		 * //String dataset_fname = "/home/xiao/test_spell_1.txt"; evaluate(sc,
		 * "/media/DataVolume1/datasets/spelling/transfered/ABODAT.643");
		 * evaluate(sc,
		 * "/media/DataVolume1/datasets/spelling/transfered/APPLING1DAT.643");
		 * evaluate(sc,
		 * "/media/DataVolume1/datasets/spelling/transfered/APPLING2DAT.643");
		 * //evaluate(sc,
		 * "/media/DataVolume1/datasets/spelling/transfered/ASHFORDDAT.643");
		 * evaluate(sc,
		 * "/media/DataVolume1/datasets/spelling/transfered/BLOORDAT.643");
		 * //evaluate(sc,
		 * "/media/DataVolume1/datasets/spelling/transfered/EXAMSDAT.643");
		 * evaluate(sc,
		 * "/media/DataVolume1/datasets/spelling/transfered/FAWTHROP1DAT.643");
		 * evaluate(sc,
		 * "/media/DataVolume1/datasets/spelling/transfered/FAWTHROP2DAT.643");
		 * evaluate(sc,
		 * "/media/DataVolume1/datasets/spelling/transfered/SHEFFIELDDAT.643");
		 * evaluate(sc,
		 * "/media/DataVolume1/datasets/spelling/transfered/TELEMARKDAT.643");
		 * evaluate(sc,
		 * "/media/DataVolume1/datasets/spelling/transfered/WINGDAT.643");
		 * evaluate(sc,
		 * "/media/DataVolume1/datasets/spelling/batch0.tab.txt_2");
		 */
	}

}
