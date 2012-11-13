package com.quest.agent.weibomonitor;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MyTokenFilter {
	
	private static int tokenLength = 2;
	
	public MyTokenFilter(){
		
	}
	
	public boolean doFilter(String str){
		
		if(str.length()<tokenLength)
			return false;
		
		Pattern pattern = Pattern.compile("^[\u4e00-\u9fa5]+$"); // 判断是不是中文
		Matcher matcher = pattern.matcher(str);
		
		return matcher.matches();
		
	}

}
