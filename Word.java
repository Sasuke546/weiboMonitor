package com.quest.agent.weibomonitor;

public class Word implements Comparable<Word>{
	
	private int num;
	
	private String word;
	
	private double tfidf;
	
	private double idf;
	
	public int compareTo(Word o) {
		if(o == null){
			return 1;
		}else{
			if(tfidf < o.tfidf) return 1;
			else if(tfidf > o.tfidf) return -1;
		}
		
		return 0;
	}
	
	public Word(){
		num = 0;
		idf = -1;
	}
	
	public Word(int tn, String tw){
		num = tn;
		word  = tw;
		idf = -1;
	}

	public int getNum() {
		return num;
	}

	public void setNum(int num) {
		this.num = num;
	}

	public String getWord() {
		return word;
	}

	public void setWord(String word) {
		this.word = word;
	}

	public double getIdf() {
		return idf;
	}

	public void setIdf(double idf) {
		this.idf = idf;
	}
	
	public double getTfIdf() {
		return tfidf;
	}

	public void setTfIdf(double tfidf) {
		this.tfidf = tfidf;
	}
	
	public void plusNum(){
		this.num++;
	}

}
