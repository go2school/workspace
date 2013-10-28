package ca.uwo.see.learn;

import java.util.ArrayList;

public class dictionaryItem {
	public String term = "";
	public ArrayList<editItem> suggestions = new ArrayList<editItem>();
	public int count = 0;
	
	public boolean equals(Object o)
	{
		return term.equals(((dictionaryItem)o).term);
	}
	
	public int hashCode()
	{
		return term.hashCode();
	}
	
	public String toString()
	{
		String ret = term;
		for(int i=0;i<suggestions.size();i++)
			ret += " " + suggestions.get(i).term;
		return ret;
	}
}
