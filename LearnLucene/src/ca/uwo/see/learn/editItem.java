package ca.uwo.see.learn;

public class editItem {
	public String term = "";
	public float distance = 0;
	
	public boolean equals(Object o)
	{
		return term.equals(((editItem)o).term);
	}
	
	public int hashCode()
	{
		return term.hashCode();
	}
}
