/*
  Copyright 2012 Quest Software, Inc.
  ALL RIGHTS RESERVED.

  This software is the confidential and proprietary information of
  Quest Software Inc. ("Confidential Information").  You shall not
  disclose such Confidential Information and shall use it only in
  accordance with the terms of the license agreement you entered
  into with Quest Software Inc.

  QUEST SOFTWARE INC. MAKES NO REPRESENTATIONS OR WARRANTIES ABOUT
  THE SUITABILITY OF THE SOFTWARE, EITHER EXPRESS OR IMPLIED,
  INCLUDING BUT NOT LIMITED TO THE IMPLIED WARRANTIES OF
  MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE, OR
  NON-INFRINGEMENT. QUEST SOFTWARE SHALL NOT BE LIABLE FOR ANY
  DAMAGES SUFFERED BY LICENSEE AS A RESULT OF USING, MODIFYING
  OR DISTRIBUTING THIS SOFTWARE OR ITS DERIVATIVES.
 */  

package com.quest.agent.weibomonitor;

import com.quest.agent.weibomonitor.samples.*;
import com.quest.agent.weibomonitor.weiboMonitorAgentPropertyWrapper.PersonID;
import com.quest.glue.api.services.*;

//import java.com.quest.agent.MyTfIdf;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.HashMap;
import java.util.Map;
import java.util.List;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.cn.smart.SmartChineseAnalyzer;
import org.apache.lucene.util.Version;

import com.quest.agent.weibomonitor.*;

import com.quest.glue.api.services.*;
import com.quest.glue.api.services.TopologyDataSubmissionService3.TopologySubmitter3;
import com.quest.glue.core.services.TopologyDataSubmitter3;


/**
 * The core implementation class of the weiboMonitorAgent agent.
 */ 
public class weiboMonitorAgentImpl implements com.quest.glue.api.agent.Agent,                                                                             
                                              weiboMonitorAgentCollectors,
 
                                              ASPPropertyListener
{

private final LogService.Logger mLogger;
private final RegistrationService mRegistrationService;
private final weiboMonitorAgentSupportBundle mBundle;

private weiboMonitorAgentPropertyWrapper mWrapper; 
private final ASPService3 mASPService; 
private final UnitService mUnitService; 
private final TopologySubmitter3 mSubmitter; 

private final weiboMonitorAgentDataProvider mDataProvider;
private HashMap<String,Word>[] map;
private final int ItemNumShow = 300;
private MyTfIdf tfidf = new MyTfIdf();

/**
 * Called by FglAM to create a new instance of this agent. This constructor is required
 * by FglAM and must be present.
 *
 * @param serviceFactory Factory used to create services for this agent
 */
public weiboMonitorAgentImpl(ServiceFactory serviceFactory) throws ServiceFactory.ServiceNotAvailableException {
    this(serviceFactory, new weiboMonitorAgentDataProviderImpl(serviceFactory)); 
}

/**
 * Creates a new instance of this agent using to provided class to collect all
 * necessary data for submission. This allows the data provided to be swapped out or
 * mocked up during unit tests.
 * <p/>
 * This is an example of one possible way to structure the agent, but it is not the only
 * way. You are free to change or remove this constructor as you see fit.  
 *
 * @param serviceFactory Factory used to create services for this agent
 * @param dataProvider The class to use to obtain all data for submission.
 */
/*pkg*/ weiboMonitorAgentImpl(ServiceFactory serviceFactory, weiboMonitorAgentDataProvider dataProvider) 
                                                throws ServiceFactory.ServiceNotAvailableException {
    LogService logService = serviceFactory.getService(LogService.class);
    mLogger = logService.getLogger(weiboMonitorAgentImpl.class);          

    mDataProvider = dataProvider;

    mRegistrationService = serviceFactory.getService(RegistrationService.class);
    // This will automatically register all the service-related listeners
    // implemented by this class.
    mRegistrationService.registerAllListeners(this);

    // This hooks the weiboMonitorAgent up to the support bundle framework and
    // and allows it to contribute information to the support bundle.
    mBundle = new weiboMonitorAgentSupportBundle(this);
    mRegistrationService.registerAllListeners(mBundle);
    
    mASPService = serviceFactory.getService(ASPService3.class); 
    mSubmitter = serviceFactory.getService(TopologyDataSubmissionService3.class).getTopologySubmitter(); 
    mUnitService = serviceFactory.getService(UnitService.class); 
    mWrapper = new weiboMonitorAgentPropertyWrapper(mASPService); 
    
    map = new HashMap[3];
    
    for(int i = 0;i < map.length;i++){
	    map[i] = new HashMap<String,Word>();
	}
    
    // Log some basic info to indicate that the agent has been created
    mLogger.log("agentVersion", "weiboMonitorAgent", "2.0.0");
}

/**
 * Called by FglAM at the end of the agent's life
 */
@Override
public void destroy() {
    mRegistrationService.unregisterAllListeners(this);
    mRegistrationService.unregisterAllListeners(mBundle);
}

/**
 * Called by FglAM to begin data collection.
 * <p/>
 * Since there are data collector(s) defined for this agent, taking action as a result
 * of this method call is optional. Each data collector method will be called by FglAM
 * when it is scheduled.
 
 */
@Override
public void startDataCollection() {
    mLogger.debug("Data collection started");
}

/**
 * Called by FglAM when data collection should be stopped.
 * <p/>
 * Since there are data collector(s) defined for this agent, taking action as a result
 * of this method call is optional.
 */
@Override
public void stopDataCollection() {
    mLogger.debug("Stopping data collection");
}

/**
 * Respond to property changes.
 * <p/>
 * This method is part of the ASPPropertyListener interface and is not required
 * by agents that do not implement it.
 * <p/>
 * Agents can receive property changes while they are running, and it is up to
 * the agent developer to ensure that modifications do not break the agent.
 */
public void propertiesChanged() {
    mLogger.debug("Property change notification received");
}

/**
 * weibo Collector Data Collector
 * 
 * @param collectionFreqInMs the collection frequency for this collector, in ms.
 */
@Override
public void collectWeibos(long collectionFreqInMs) {
	
	String personID = getPersonID();
	
	ModelRoot root = collect(collectionFreqInMs,"PeopleAreSaying","select * from status where `read`=0;",0);
	
	submitData(collectionFreqInMs, root);
	
	root = collect(collectionFreqInMs,"Person","select * from status2 where id="+personID+" and `read`=0;",1);
	
	submitData(collectionFreqInMs, root);
	
	root = collect(collectionFreqInMs,"Group","select * from status2 where relation="+personID+" and `read`=0;",2);
	
	submitData(collectionFreqInMs, root);
	
    mLogger.debug("weibo Collector collector invoked");
}

private ModelRoot collect(long collectionFreqInMs,String groupName,String sqlQuery,int groupID) { 
	  Weibo agentRoot = new Weibo(groupName); 
	  //TODO: collect data and populate the data collected to model(topology) 
	   
	  //List<UrlList> urlList = mWrapper.getUrlList();
	  
	  Analyzer ca = new SmartChineseAnalyzer(Version.LUCENE_CURRENT);
		
	  try {
		  
		  SQLProcess sql = new SQLProcess();
		  
		  ResultSet res = sql.executeQuery(sqlQuery);
		  
	      MyTokenFilter tkFilter = new MyTokenFilter();
	      
		  while(res.next()) {
			  Reader sentence = new StringReader(res.getString("status").toString());
			  
			  String weiboID = res.getObject("weiboId").toString();
			  
			  if(groupID==0)
				  sql.execute("update status set status.read=1 where weiboId="+weiboID+";");
			  else
				  sql.execute("update status2 set status2.read=1 where weiboId="+weiboID+";");
			  
			  TokenStream ts = ca.tokenStream("", sentence);
			  try {
					while (ts.incrementToken()) {
							String ss[] = ts.toString().split(",");
							ss[0] = ss[0].replace("(", "");
							if(tkFilter.doFilter(ss[0]))
							{
								if(!map[groupID].containsKey(ss[0]))
									map[groupID].put(ss[0], new Word(1,ss[0]));
								else
									map[groupID].get(ss[0]).plusNum();
							}
					    }
				} catch (IOException e){
					mLogger.debug2("error occurred while incrementToken", e); 
				}
		  }	
	  } catch (SQLException e) {
		  // TODO Auto-generated catch block
		  e.printStackTrace();
	  }
	  
	  Word[] wordList = tfidf.doProcess(map[groupID]);
	  
	  int mapsize = map[groupID].size();
	  for(int i=0;i<Math.min(mapsize,ItemNumShow);i++){
		  
			collectWeibo(wordList[i].getWord(),wordList[i].getNum(),wordList[i].getTfIdf(),wordList[i].getIdf(),agentRoot);
			
		}
		
	  return agentRoot; 
	}


private void collectWeibo(String word, int num, double tfidf,double idf,Weibo agentRoot) {
	
	MonitoredWeibo mw = new MonitoredWeibo(word);
	
	mw.setNumber(new Double(num));
	mw.setPercentage(new Double(tfidf));
	mw.setIdf(idf);
	
	agentRoot.getInstances().add(mw); 
	
}

private String getPersonID(){
	
	String pid="";
	try {
		  
		  SQLProcess sql = new SQLProcess();
		  
		  List<PersonID> person = mWrapper.getPersonID();
		  
		  Iterator<PersonID> it = person.iterator();
		  
		  pid = it.next().getId();
		  
		  sql.execute("delete from attention;");
		  sql.execute("insert into attention values("+pid+");");
		  
	  } catch (SQLException e) {
		  // TODO Auto-generated catch block
		  e.printStackTrace();
	  }
	return pid;
}

private void submitData(long collectionFreqInMs, ModelRoot root) { 
	  if(root != null){ 
	    long timestamp = System.currentTimeMillis(); 
	    try { 
	      root.submit(mSubmitter, mUnitService, collectionFreqInMs, timestamp ); 
	    } catch (TopologyException e) { 
	      mLogger.debug("error occurred while submit date", e); 
	    } 
	  }   
	} 


}