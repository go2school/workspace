package ca.uwo.see.learn;

import org.apache.commons.codec.language.DoubleMetaphone;

public class DoubleMetaPhoneTest {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String [] data = {"problem", "problam", "proble", "promblem", "proplen"};
		DoubleMetaphone dmp = new DoubleMetaphone();
		for(int i=0;i<data.length;i++)
		{
			System.out.println(data[i] + " " + dmp.encode(data[i]) + " " + dmp.doubleMetaphone(data[i], true) + " " + dmp.doubleMetaphone(data[i], false));
			for(int j=i+1;j<data.length;j++)
			{
				
				if(dmp.isDoubleMetaphoneEqual(data[i], data[j]))
						System.out.println(i+" " + j);
			}
		}
	}

}
