package ca.uwo.see.learn;

import java.util.ArrayList;
import java.util.Hashtable;

public class CandidateGenerator {

	public final ArrayList<String> edits(String word) {
		ArrayList<String> result = new ArrayList<String>();
		//delete
		for(int i=0; i < word.length(); ++i) result.add(word.substring(0, i) + word.substring(i+1));
		//insert
		for(int i=0; i < word.length()-1; ++i) result.add(word.substring(0, i) + word.substring(i+1, i+2) + word.substring(i, i+1) + word.substring(i+2));
		//
		for(int i=0; i < word.length(); ++i) for(char c='a'; c <= 'z'; ++c) result.add(word.substring(0, i) + String.valueOf(c) + word.substring(i+1));
		//
		for(int i=0; i <= word.length(); ++i) for(char c='a'; c <= 'z'; ++c) result.add(word.substring(0, i) + String.valueOf(c) + word.substring(i));
		return result;
	}
	
	//only return the deletion operation
	public static ArrayList<editItem> editsRec(String word, float editdistance, float maxEditDistance, boolean recursion)
	{
		editdistance++;
		ArrayList<editItem> list = new ArrayList<editItem>();
		if(word.length() > 1)
		{
			for(int i=0;i<word.length();i++)
			{
				editItem delete = new editItem();
				delete.term = word.substring(0, i) + word.substring(i+1);
				delete.distance = editdistance;
				if(!list.contains(delete))
				{
					list.add(delete);
					if(recursion && editdistance < maxEditDistance)
					{
						ArrayList<editItem> editlist = editsRec(delete.term, editdistance, maxEditDistance, recursion);
						for(int j=0;j<editlist.size();j++)
						{
							if(!list.contains(editlist.get(j)))
								list.add(editlist.get(j));
						}
					}					
				}
			}
		}
		return list;
	}
	
	
	public static void testRecEdit()
	{
		String word = "good";
		ArrayList<editItem> editlist = editsRec(word, 0, 2, true);
		for(int i=0;i<editlist.size();i++)
			System.out.println(editlist.get(i).term);
	}
	
	//*****************************
    // Compute Levenshtein distance: see org.apache.commons.lang.StringUtils#getLevenshteinDistance(String, String)
    //*****************************
    public float getDistance (String target, String other) {
      char[] sa;
      int n;
      int p[]; //'previous' cost array, horizontally
      int d[]; // cost array, horizontally
      int _d[]; //placeholder to assist in swapping p and d
      
        /*
           The difference between this impl. and the previous is that, rather
           than creating and retaining a matrix of size s.length()+1 by t.length()+1,
           we maintain two single-dimensional arrays of length s.length()+1.  The first, d,
           is the 'current working' distance array that maintains the newest distance cost
           counts as we iterate through the characters of String s.  Each time we increment
           the index of String t we are comparing, d is copied to p, the second int[].  Doing so
           allows us to retain the previous cost counts as required by the algorithm (taking
           the minimum of the cost count to the left, up one, and diagonally up and to the left
           of the current cost count being calculated).  (Note that the arrays aren't really
           copied anymore, just switched...this is clearly much better than cloning an array
           or doing a System.arraycopy() each time  through the outer loop.)

           Effectively, the difference between the two implementations is this one does not
           cause an out of memory condition when calculating the LD over two very large strings.
         */

        sa = target.toCharArray();
        n = sa.length;
        p = new int[n+1]; 
        d = new int[n+1]; 
      
        final int m = other.length();
        if (n == 0 || m == 0) {
          if (n == m) {
            return 1;
          }
          else {
            return 0;
          }
        } 


        // indexes into strings s and t
        int i; // iterates through s
        int j; // iterates through t

        char t_j; // jth character of t

        int cost; // cost

        for (i = 0; i<=n; i++) {
            p[i] = i;
        }

        for (j = 1; j<=m; j++) {
            t_j = other.charAt(j-1);
            d[0] = j;

            for (i=1; i<=n; i++) {
                cost = sa[i-1]==t_j ? 0 : 1;
                // minimum of cell to the left+1, to the top+1, diagonally left and up +cost
                d[i] = Math.min(Math.min(d[i-1]+1, p[i]+1),  p[i-1]+cost);
            }

            // copy current distance counts to 'previous row' distance counts
            _d = p;
            p = d;
            d = _d;
        }

        // our last action in the above loop was to switch d and p, so p now
        // actually has the most recent cost counts
        return 1.0f - ((float) p[n] / Math.max(other.length(), sa.length));
    }
    
    // Damerau¨CLevenshtein distance algorithm and code
    // from http://en.wikipedia.org/wiki/Damerau%E2%80%93Levenshtein_distance
    public static int DamerauLevenshteinDistance(String str_source, String str_target)
    {
        int m = str_source.length();
        int n = str_target.length();
        int[][] H = new int[m + 2][n + 2];
 
        char [] source = str_source.toCharArray();
        char [] target = str_target.toCharArray();
        
        int INF = m + n;
        H[0][0] = INF;
        for (int i = 0; i <= m; i++) { H[i + 1][ 1] = i; H[i + 1][0] = INF; }
        for (int j = 0; j <= n; j++) { H[1][j + 1] = j; H[0][j + 1] = INF; }
 
        Hashtable<Integer, Integer> sd = new Hashtable<Integer, Integer>();

        for(int i=0;i<source.length;i++)
        {
            if (!sd.containsKey(source[i] - 32))
                sd.put(source[i] - 32, 0);
        }
        for(int i=0;i<target.length;i++)
        {
            if (!sd.containsKey(target[i] - 32))
                sd.put(target[i] - 32, 0);
        }
 
        for (int i = 1; i <= m; i++)
        {
            int DB = 0;
            for (int j = 1; j <= n; j++)
            {
                int i1 = sd.get(target[j-1] - 32);
                int j1 = DB;
 
                if (source[i - 1] == target[j - 1])
                {
                    H[i + 1][j + 1] = H[i][j];
                    DB = j;
                }
                else
                {
                    H[i + 1][j + 1] = Math.min(H[i][j], Math.min(H[i + 1][j], H[i][j + 1])) + 1;
                }
 
                H[i + 1][j + 1] = Math.min(H[i + 1][j + 1], H[i1][j1] + (i - i1 - 1) + 1 + (j - j1 - 1));
            }
             
            sd.put(source[i - 1] - 32, i);
        }
        return H[m + 1][n + 1];
    }
    
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		testRecEdit();
	}

}
