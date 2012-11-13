package com.quest.agent.weibomonitor;


import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

public class MyTfIdf {
	
	private HashMap<String,Double> mymap;  // dictionary
	
	public MyTfIdf(){
		mymap = new HashMap<String,Double>();
	}
	
	public Word[] doProcess(HashMap<String,Word> tmap){
		
		//List<Word> wordList = new ArrayList<Word>();
		int mapsize = tmap.size();
		int wordidfSize = 0;
		Word[] words = new Word[mapsize];
		SQLProcess sql = new SQLProcess();
		
		try
		{
			ResultSet ResnumIDF = sql.executeQuery("select count(*) from wordidf;");
			ResnumIDF.next();
			wordidfSize = ResnumIDF.getInt(1);
		}catch (SQLException e) {
			  e.printStackTrace();
		}
		
		Iterator itr = tmap.keySet().iterator();
		int index = 0;
		while(itr.hasNext())
		{
			String tw = itr.next().toString();
			int num = tmap.get(tw).getNum();
			
			Word newword = new Word(num,tw);
			
			try{
				if(!mymap.containsKey(tw))
				{
					ResultSet res = sql.executeQuery("select * from wordidf where word='"+tw+"';");
					if(res.next())
					{
						double idf = res.getDouble("idf");
						newword.setTfIdf(num*idf);
						newword.setIdf(idf);
						mymap.put(tw, new Double(idf));
					}
					else
					{
						newword.setTfIdf(num * Math.log(wordidfSize*1.0/num));
						newword.setIdf(Math.log(wordidfSize*1.0/num));
						String sqlSta = String.format("insert into wordidf (word,idf) values ('%s',%.2f);",tw,Math.log(wordidfSize*1.0/num));
						wordidfSize++;
						mymap.put(tw, new Double(Math.log(wordidfSize*1.0/num)));
						sql.execute(sqlSta);
					}
				}
				else
				{
					double idf = mymap.get(tw);
					newword.setTfIdf(num*idf);
					newword.setIdf(idf);
				}
			}catch (SQLException e) {
				  // TODO Auto-generated catch block
				  e.printStackTrace();
			}
			
			words[index++] = newword;			
		}
		
		Arrays.sort(words);
		
		return words;
		
	}

}
