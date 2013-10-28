package ca.uwo.see.learn;

public class suggestItem {
	public String term = "";
	public float distance = 0;
	public int count = 0;
	
	public float metaphone_distance = 0;
	
	public boolean equals(Object o)
	{
		return term.equals(((suggestItem)o).term);
	}
	
	public int hashCode()
	{
		return term.hashCode();
	}
}
