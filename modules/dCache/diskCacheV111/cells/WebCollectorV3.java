// $Id: WebCollectorV3.java,v 1.24 2005-12-14 09:59:10 tigran Exp $Cg

package diskCacheV111.cells ;

import dmg.cells.services.login.LoginBrokerInfo ;
import diskCacheV111.poolManager.PoolManagerCellInfo ;

import java.util.* ;
import java.text.* ;
import java.io.* ;
import dmg.cells.nucleus.* ;
import dmg.util.* ;


import diskCacheV111.pools.* ;

public class WebCollectorV3 extends CellAdapter implements Runnable {
   private CellNucleus _nucleus        = null ;
   private Args       _args            = null ;
   private HashMap    _infoMap         = new HashMap() ;
   private Object     _lock            = new Object() ;
   private Object     _infoLock        = new Object() ;
   private Thread     _collectThread   = null ;
   private Thread     _senderThread    = null ;
   private String     _bgColor         = "yellow" ;
   private long       _counter         = 0 ;
   private boolean    _debug           = false ;
   private String []  _rowColors = { "\"#efefef\"" , "\"#bebebe\"" } ;
   private int        _repeatHeader    = 30 ;
   private String []  _loginBrokerTable = null ;
   private  class SleepHandler {
      private boolean enabled         = true ;
      private boolean mode            = true ;
      private long    started         = 0L ;
      private long    shortPeriod     =  20000L ;
      private long    regularPeriod   = 120000L ;
      private long    retentionFactor = 4 ;
      private SleepHandler( boolean aggressive ){ 
          this.enabled = aggressive ; 
          this.mode    = aggressive ;
      }
      private synchronized void sleep() throws InterruptedException {
          long start = System.currentTimeMillis() ;
          super.wait( mode ? shortPeriod/2 : regularPeriod/2 ) ;
          dsay("Woke up after "+(System.currentTimeMillis()-start)+" millis");
      }
      private synchronized void setShortPeriod( long shortPeriod ){
         this.shortPeriod = shortPeriod ;
         this.notifyAll() ;
      }
      private synchronized void setRegularPeriod( long regularPeriod ){
         this.regularPeriod = regularPeriod ;
         this.notifyAll() ;
      }
      private synchronized void topologyChanged( boolean modified ){
         // say("Topology changed : "+modified);
         if( ! enabled )return ;
         if( modified ){
             started = System.currentTimeMillis() ;
             if( mode )return ;
             mode = true ;
             notifyAll() ;
             say("Aggressive changed to ON");
             
         }else{
             if( ! mode )return ;
             if( ( System.currentTimeMillis() - started ) >
                 ( shortPeriod * retentionFactor ) ){

                 mode = false ;
                 notifyAll() ;
                 say("Aggressive changed to OFF");
             }

         }
      
      }
      public String toString(){
         StringBuffer sb = new StringBuffer() ;
         sb.append("E=").append(enabled).append(";A=").append(mode).
            append(";S=").append(shortPeriod).append(";Ret").append(retentionFactor).
            append(";R=").append(regularPeriod);
         return sb.toString();
      }
   }   ;
   private SleepHandler     _sleepHandler = null ;
   private SimpleDateFormat _formatter    = new SimpleDateFormat ("MM/dd HH:mm:ss");
     
   private class CellQueryInfo {
   
       private String   _destination = null ;
       private long     _diff        = -1 ;
       private long     _start       = 0 ;
       private CellInfo _info        = null ;
       private CellMessage _message  = null ;
       private long        _lastMessage = 0 ;
       private CellQueryInfo( String destination ){
           _destination = destination ;
	   //_destination = new CellPath(destination).getCellName();
           _message = new CellMessage( new CellPath(_destination) , "xgetcellinfo" ) ;
       }
       private String      getName(){ return _destination ; }
       private CellInfo    getCellInfo(){ return _info ; }
       private long        getPingTime(){ return _diff ; }
       private CellMessage getCellMessage(){
          _start = System.currentTimeMillis() ;
          return _message ;
       }
       private void infoArrived( CellInfo info ){
          _info = info ;
          _diff = ( _lastMessage = System.currentTimeMillis() ) - _start ;
       }
       private boolean isOk(){ 
         return ( System.currentTimeMillis() - _lastMessage) < (3*_sleepHandler.regularPeriod) ;
       }
   }
   public WebCollectorV3( String name , String args )throws Exception {
      super( name , WebCollectorV3.class.getName(), args , false ) ;
      _args    = getArgs() ;
      _nucleus = getNucleus() ;
      try{
          _debug = _args.getOpt("debug") != null ;
          
          String optionString = null ;
          try{
              optionString  = _args.getOpt("repeatHeader" ) ;
              _repeatHeader = Math.max( 0 , Integer.parseInt( optionString ) ) ;
          }catch(Exception ee ){
              esay( "Parsing error in repeatHader command : "+optionString);
          }
          say("Repeat header set to "+_repeatHeader);
          
          optionString = _args.getOpt("aggressive") ;
          boolean aggressive =
              ( optionString != null ) && 
              ( optionString.equals("off") || optionString.equals("false") ) ;
              
          aggressive = ! aggressive ;
          
          say("Agressive mode : "+aggressive ) ;
          
          _sleepHandler = new SleepHandler( aggressive ) ;
              
          
          for( int i = 0 ; i < _args.argc() ; i++ )addQuery( _args.argv(i) )  ;
          
          String loginBrokers = _args.getOpt("loginBroker");
          if( ( loginBrokers != null ) && ( loginBrokers.length() > 0 ) ){
             StringTokenizer st = new StringTokenizer( loginBrokers , "," ) ;
             ArrayList list = new ArrayList() ;
             while( st.hasMoreTokens() ){
                String cellName = st.nextToken() ;
                say("Login Broker : "+cellName);
                list.add( cellName ) ;
                addQuery( cellName ) ;
             }
             _loginBrokerTable = (String [])list.toArray( new String[0] ) ;
          }
          ( _senderThread  = _nucleus.newThread( this , "sender" ) ).start() ;
          say("Sender started" ) ;
          say("Collector will be started a bit delayed" ) ;
          _nucleus.newThread(
              new Runnable(){
                 public void run(){
                    try{
                       Thread.sleep( 30000L ) ;
                    }catch(Exception ee){
                       return ;
                    }
                    _collectThread = _nucleus.newThread( WebCollectorV3.this , "collector" )  ;
                    _collectThread.start() ;
                    say("Collector now started as well");
                 }
              },
              "init" 
          ).start() ;
             
          
      }catch(Exception ee ){
          esay( "<init> of WebCollector reports : "+ee.getMessage());
          esay(ee);
          start() ;
          kill() ;
          throw ee ;
      }  
      start() ;
   }
   public void dsay( String message ){
      pin(message);
      if( _debug )super.say(message);
   }
   public void say( String message ){
      pin(message);
      super.say(message);
   }
   public void esay( String message ){
      pin(message);
      super.say(message);
   }
   private synchronized boolean addQuery( String destination ){
      if( _infoMap.get( destination ) != null )return false ;
      say( "!!! Adding "+destination ) ;
      _infoMap.put( destination , new CellQueryInfo( destination ) ) ;
      return true ;
   }
   private synchronized void removeQuery( String destination ){
      say( "Removing "+destination ) ;
      _infoMap.remove( destination ) ;
      return ;
   }
   public void run(){
      Thread x = Thread.currentThread() ;
      if( x == _senderThread )runSender() ;
      else runCollector() ;
   }
   private void runCollector(){
     while( ! Thread.currentThread().interrupted() ){
        synchronized( _infoLock ){
           preparePage() ;	
        }
        try{
            _sleepHandler.sleep();
        }catch(InterruptedException iie ){
           say("Collector Thread interrupted" ) ;
           break ;
        }
     }
   
   }
   private void runSender(){
     //CellMessage loginBrokerMessage = new CellMessage( new CellPath
     

      while( ! Thread.currentThread().interrupted() ){
         _counter++ ;
         if( _loginBrokerTable != null ){         
            for( int i = 0 ; i < _loginBrokerTable.length ; i++ ){
               try{
                  CellPath path = new CellPath( _loginBrokerTable[i] ) ;
                  dsay("Sending LoginBroker query to : "+path ) ;
                  sendMessage( new CellMessage( path , "ls -binary" ) );              
               }catch( Exception ee ){

               }
            }
         }
         //sendMessage( loginBrokerMessage ) ;
         synchronized( _infoLock ){
            Iterator i = _infoMap.values().iterator() ;
            while( i.hasNext() ){
               CellQueryInfo info = (CellQueryInfo)i.next() ;
               try{
                  dsay("Sending query to : "+info.getName() ) ;
                  sendMessage( info.getCellMessage() ) ;
               }catch( Exception ee ){

               }
            }
         }
         try{
            _sleepHandler.sleep() ;
         }catch(InterruptedException iie ){
            say("Sender Thread interrupted" ) ;
            break ;
         }
      }
   }
   public void messageArrived( CellMessage message ){
      CellPath path = message.getSourcePath() ;
      String destination = (String)path.getCellName() ;
      CellQueryInfo info = (CellQueryInfo)_infoMap.get(destination);
      if( info == null ){
         dsay("Unexpected reply arrived from : "+path ) ;
         return ;
      }
      Object reply = message.getMessageObject() ;
      
      int modified = 0 ;
      
      if( reply instanceof CellInfo ){
          dsay("CellInfo : "+((CellInfo)reply).getCellName());
          info.infoArrived((CellInfo)reply);
      }
      if( reply instanceof diskCacheV111.poolManager.PoolManagerCellInfo ){
         String [] poolList = ((PoolManagerCellInfo)reply).getPoolList() ;
         synchronized( _infoLock ){
            for( int i = 0 ; i < poolList.length ; i++ )if( addQuery(poolList[i]) )modified++ ;
         }
      }
      
      if( reply instanceof dmg.cells.services.login.LoginBrokerInfo[] ){
         dsay("Login broker reply : "+((dmg.cells.services.login.LoginBrokerInfo[])reply).length);
         LoginBrokerInfo [] brokerInfos = (LoginBrokerInfo [])reply ;
         synchronized( _infoLock ){
             for( int i = 0 ; i < brokerInfos.length ; i++ ){
                 String dest = brokerInfos[i].getCellName()  ;
                 dsay("Login broker reports : "+dest);
                 if( addQuery(dest) )modified++ ;
             }
         }
      }
      _sleepHandler.topologyChanged( modified > 0 );
   }
   
   public String ac_set_bgcolor_$_1(Args args ){
      _bgColor = args.argv(0) ;
      return "" ;
   }
   public String hh_set_repeat_header = "<repeatHeaderCount>|0" ;
   public String ac_set_repeat_header_$_1( Args args ){
      _repeatHeader = Integer.parseInt( args.argv(0) ) ;
      return "" ;
   }
   private HashMap _poolGroup = new HashMap() ;
   public String hh_define_poolgroup = "<poolgroup> [poolName | /regExpr/ ] ... " ;
   public String ac_define_poolgroup_$_1_99( Args args ){
       String poolGroupName = args.argv(0) ;
       synchronized( _poolGroup ){
           Map map = (Map)_poolGroup.get(  poolGroupName ) ;
           if( map == null )_poolGroup.put( poolGroupName , map = new HashMap() );

           for( int i = 0 , n = args.argc() - 1 ; i < n ; i++ ){
              String poolName = args.argv(i) ;
           }
       }
       return "" ;
  
   }
   public String hh_watch = "<CellAddress> [...]" ;
   public String ac_watch_$_1_99( Args args ){
      for( int i = 0 ; i < args.argc() ; i++ )addQuery( args.argv(i) ) ;
      return "" ;
   }
   public String hh_dump_info = "[minPingTime] # dumps all info about watched cells";
   public String ac_dump_info_$_0_1( Args args) {
       Map  sortedMap = new TreeMap( _infoMap ) ;
       Iterator i        = sortedMap.values().iterator() ;
       long     pingTime = 0 ;
       long     minPingTime = 0;
       CellInfo cellInfo = null ;
       StringBuffer buf = new StringBuffer();
       if( args.argc() > 0 ) {
           minPingTime  = Long.parseLong(args.argv(0));
       }
       
       for( int n = 0 ; i.hasNext() ; n++ ){
           try{
               CellQueryInfo info = (CellQueryInfo)i.next() ;
               cellInfo = info.getCellInfo() ;
               pingTime = info.getPingTime() ;
               if( pingTime > minPingTime) {
                   if( info.isOk() ){
                       buf.append("" + cellInfo.getDomainName() + " " + cellInfo + " " + pingTime + "\n");
                   }else{
                       buf.append("" + cellInfo.getDomainName() + " " + cellInfo + "\n");
                   }
               }
           }catch(Exception ee){
               esay(ee);
               continue ;
           }
           
       }
       return buf.toString();
   }

   public String hh_unwatch = "<CellAddress> [...]" ;
   public String ac_unwatch_$_1_99( Args args ){
      for( int i = 0 ; i < args.argc() ; i++ )removeQuery( args.argv(i) ) ;
      return "" ;
   }
   public String hh_set_interval = "[<pingInteval/sec>] [-short=<aggressiveInterval>]" ;
   public String ac_set_interval_$_1( Args args ){
   
      if( args.argc() > 0 ){
          _sleepHandler.setRegularPeriod( 1000L * Long.parseLong(args.argv(0) ) ) ;
      }
      String opt = args.getOpt("short") ;
      if( opt != null ){
          _sleepHandler.setShortPeriod( 1000L * Long.parseLong( opt ) ) ;
      }
      long _interval = 1000 * Long.parseLong(args.argv(0));
      return "Interval set to "+_interval+" [msecs]" ;
   }
   private void printEagle( StringBuffer sb ){
      sb.append("<html>\n<head><title>dCache ONLINE</title></head>\n");
      sb.append("<body background=\"/images/bg.jpg\" link=red vlink=red alink=red>\n");
      
//      sb.append( "<table border=0 cellpadding=10 cellspacing=0 width=\"90%\">\n");
//      sb.append( "<tr><td align=center valign=center width=\"1%\">\n" ) ;
      sb.append( "<a href=\"/\"><img border=0 src=\"/images/eagleredtrans.gif\"></a>\n");
      sb.append( "<br><font color=red>Birds Home</font>\n" ) ;
//      sb.append( "</td><td align=center>\n" ) ;
//      sb.append( "<h1><font color=blue>dCache Billing</font></h1>");
//      sb.append( "</td></tr></table>");

      sb.append("<center><img src=\"/images/eagle-grey.gif\"></center>\n");
      sb.append("<p>\n");
   }
   private void printPoolTableHeader( StringBuffer sb ){
      sb.append( "<center>\n<table border=1 cellpadding=4 cellspacing=0 width=\"90%\">\n");
      sb.append("<tr>\n");
      String color = "\"#115259\"" ;
      sb.append("<td bgcolor="+color+" align=center><font color=white>"+"CellName"+"</font></td>\n") ;
      sb.append("<td bgcolor="+color+" align=center><font color=white>"+"DomainName"+"</font></td>\n") ;
      sb.append("<td bgcolor="+color+" align=center><font color=white>"+"Total Space/MB"+"</font></td>\n") ;
      sb.append("<td bgcolor="+color+" align=center><font color=white>"+"Free Space/MB"+"</font></td>\n") ;
      sb.append("<td bgcolor="+color+" align=center><font color=white>"+"Precious Space/MB"+"</font></td>\n") ;
      sb.append("<td bgcolor="+color+" align=center>").
         append("<font color=white>Layout  </font>").
         append("<font color=red>(precious/</font>").
         append("<font color=green>used/</font>").
         append("<font color=yellow>free)</font>").
         append("</td>\n") ;
      sb.append("</tr>\n");
   }
   private static final int HEADER_TOP    =   0 ;
   private static final int HEADER_MIDDLE =   1 ;
   private static final int HEADER_BOTTOM =   2 ;
   private class ActionHeaderExtension {
      private Map _map = null; 
      private ActionHeaderExtension( Map map ){
         _map = map == null ? new TreeMap() : new TreeMap(map) ; // sort alphabeta
      }
      public String toString(){ return _map.toString() ; }
      int [] [] getSortedMovers( Map moverMap ){
         int [] [] rows = new int[_map.size()][] ;
         int i = 0 ;
         if( moverMap == null ){
            for( i = 0 ; i < _map.size() ;i++ ){
               rows[i] = new int[3] ;
               for( int l = 0; l < 3 ; l++ )rows[i][l] = -1 ;
            }
         
         }else{
            for( Iterator it = _map.keySet().iterator() ; it.hasNext() ; i++){

               rows[i] = new int[3] ;
               PoolCostInfo.PoolQueueInfo mover = (PoolCostInfo.PoolQueueInfo)moverMap.get(it.next().toString()) ;
               if( mover == null  ){
                  for( int l = 0; l < 3 ; l++ )rows[i][l] = -1 ;
               }else{
                  rows[i][0] = mover.getActive() ;
                  rows[i][1] = mover.getMaxActive() ;
                  rows[i][2] = mover.getQueued() ;
               }            
            }
         }
         return rows ;
      }
      public Set getSet(){ return _map.keySet() ; }
      public Map getTotals(){ return _map ; }
   }
   private void printPoolActionTableHeader( StringBuffer sb , ActionHeaderExtension ext , int position ){
      String color0 = "\"#0000FF\"" ;
      String color1 = "\"#0099FF\"" ;
      String color2 = "\"#00bbFF\"" ;
      String color3 = "\"#115259\"" ;
     
      int [] regularProgram = { 0 , 1 , 2 , 3 } ;
      int [] reverseProgram = { 0 , 3 , 2 , 1 } ;
      int [] middleProgram  = { 0 , 3 , 2 , 1 , 2 , 3 } ;

      int [] program = position == HEADER_TOP ? regularProgram : position == HEADER_MIDDLE ? middleProgram : reverseProgram ;
      Set moverSet = ext != null ? ext.getSet() : null ;
      int diff = moverSet == null ? 0 : moverSet.size() ;
      for( int i = 0 ; i < program.length ; i++ ){
        switch( program[i] ){
           case 0 :
              int rowspan = program.length / 2 ;
              sb.append("<tr>\n");
              sb.append("<td rowspan=").append(rowspan).append(" valign=center bgcolor=").append(color3).
                 append(" align=center><font color=white>").append("CellName").append("</font></td>\n") ;

              sb.append("<td rowspan=").append(rowspan).append(" valign=center bgcolor=").append(color3).
                 append(" align=center><font color=white>").append("DomainName").append("</font></td>\n") ;
           break ;
           case 1 :
              sb.append("<td colspan=3 bgcolor=").append(color3).
                 append(" align=center><font color=white>").append("Movers").append("</font></td>\n") ;
              sb.append("<td colspan=3 bgcolor=").append(color3).
                 append(" align=center><font color=white>").append("Restores").append("</font></td>\n") ;
              sb.append("<td colspan=3 bgcolor=").append(color3).
                 append(" align=center><font color=white>").append("Stores").append("</font></td>\n") ;
              sb.append("<td colspan=3 bgcolor=").append(color3).
                 append(" align=center><font color=white>").append("P2P-Server").append("</font></td>\n") ;
              sb.append("<td colspan=3 bgcolor=").append(color3).
                 append(" align=center><font color=white>").append("P2P-Client").append("</font></td>\n") ;
                 
              if( moverSet != null ){
                 for( Iterator it = moverSet.iterator() ; it.hasNext() ; ){
                    sb.append("<td colspan=3 bgcolor=").append(color3).
                       append(" align=center><font color=white>").append(it.next().toString()).append("</font></td>\n") ;  
                 }
              }
              sb.append("</tr>\n");
           break ;
           case 2 :
              sb.append("<tr>");
           break ;
           case 3 :
              for( int h = 0 , n = 5 + diff ; h < n ; h++ )
              sb.append("<td bgcolor=").append(color1).
                 append(" align=center><font color=white>").
                 append("Active</font></td>\n").
                 append("<td bgcolor=").append(color1).
                 append(" align=center><font color=white>").
                 append("Max</font></td>\n").
                 append("<td bgcolor=").append(color2).
                 append(" align=center><font color=white>").
                 append("Queued</font></td>\n") ;

              sb.append("</tr>\n");
           break ;
          }
      }
   }
   private void printCellTableHeader( StringBuffer sb ){
      sb.append( "<center>\n<table border=1 cellpadding=4 cellspacing=0 width=\"90%\">\n");
      sb.append("<tr>\n");
      String color = "\"#115259\"" ;
      sb.append("<td bgcolor="+color+" align=center><font color=white>"+"CellName"+"</font></td>\n") ;
      sb.append("<td bgcolor="+color+" align=center><font color=white>"+"DomainName"+"</font></td>\n") ;
      sb.append("<td bgcolor="+color+" align=center><font color=white>"+"RP"+"</font></td>\n") ;
      sb.append("<td bgcolor="+color+" align=center><font color=white>"+"TH"+"</font></td>\n") ;
      sb.append("<td bgcolor="+color+" align=center><font color=white>"+"Ping"+"</font></td>\n") ;
      sb.append("<td bgcolor="+color+" align=center><font color=white>"+"Creation Time"+"</font></td>\n") ;
      sb.append("<td bgcolor="+color+" align=center><font color=white>"+"Version"+"</font></td>\n") ;
      sb.append("</tr>\n");
   }
   private void printCellInfoRow( CellInfo info , long ping , 
                                  StringBuffer sb , String color ){
//      String color = "\"#10B0EE\"" ;
      String dateString = _formatter.format(info.getCreationTime()) ;
      sb.append("<tr>\n");
      sb.append("<td bgcolor="+color+" align=center>"+info.getCellName()+"</td>\n") ;
      sb.append("<td bgcolor="+color+" align=center>"+info.getDomainName()+"</td>\n") ;
      sb.append("<td bgcolor="+color+" align=center>"+info.getEventQueueSize()+"</td>\n") ;
      sb.append("<td bgcolor="+color+" align=center>"+info.getThreadCount()+"</td>\n") ;
      sb.append("<td bgcolor="+color+" align=center>"+ping+" msec"+"</td>\n") ;
      sb.append("<td bgcolor="+color+" align=center>"+dateString+"</td>\n") ;
      try{ // may throw 'noSuchMethodException
          sb.append("<td bgcolor="+color+" align=center>"+info.getCellVersion()+"</td>\n") ;
      }catch( Exception ee ){
         sb.append("<td bgcolor="+color+" align=center>not-implemented</td>\n") ;
      }
      sb.append("</tr>\n");
   }
////////////////////////////////////////////////////////////////////////////////////////////
//
//               the pool queue info table(s)
//
   /**
    *       convert the pool cost info (xgetcellinfo) into the 
    *       int [] []   array.
    */
   private int [] []  decodePoolCostInfo( PoolCostInfo costInfo ){
       
      try{
         
         PoolCostInfo.PoolQueueInfo mover     = costInfo.getMoverQueue() ;
         PoolCostInfo.PoolQueueInfo restore   = costInfo.getRestoreQueue() ;
         PoolCostInfo.PoolQueueInfo store     = costInfo.getStoreQueue() ;
         PoolCostInfo.PoolQueueInfo p2pServer = costInfo.getP2pQueue() ;
         PoolCostInfo.PoolQueueInfo p2pClient = costInfo.getP2pClientQueue() ;
         
         int [] [] rows = new int[5][] ;
         
         rows[0] = new int[3] ;
         rows[0][0] = mover.getActive() ;
         rows[0][1] = mover.getMaxActive() ;
         rows[0][2] = mover.getQueued() ;
         
         rows[1] = new int[3] ;
         rows[1][0] = restore.getActive() ;
         rows[1][1] = restore.getMaxActive() ;
         rows[1][2] = restore.getQueued() ;
         
         rows[2] = new int[3] ;
         rows[2][0] = store.getActive() ;
         rows[2][1] = store.getMaxActive() ;
         rows[2][2] = store.getQueued() ;
         
         if( p2pServer == null ){
            rows[3] = null ;
         }else{
            rows[3] = new int[3] ;
            rows[3][0] = p2pServer.getActive() ;
            rows[3][1] = p2pServer.getMaxActive() ;
            rows[3][2] = p2pServer.getQueued() ;
         }
         
         rows[4] = new int[3] ;
         rows[4][0] = p2pClient.getActive() ;
         rows[4][1] = p2pClient.getMaxActive() ;
         rows[4][2] = p2pClient.getQueued() ;

         return rows ;

      }catch( Exception e){
         esay(e);
         return null ;
      }	 
   }
   private void printPoolInfoRow( PoolCellInfo cellInfo , StringBuffer sb , String color ){
      long mb     = 1024*1024 ;
      int  maxPix = 300 ;
      
      PoolCostInfo.PoolSpaceInfo info = cellInfo.getPoolCostInfo().getSpaceInfo() ;
      
      long total     = info.getTotalSpace() ;
      long freespace = info.getFreeSpace() ;
      long precious  = info.getPreciousSpace() ;
      long removable = info.getRemovableSpace() ;
//      String color    = "\"#10B0EE\"" ;
//      String errcolor = "\"#00D5FF\"" ;

      String errcolor = color ;
      String prefix   = "<td bgcolor="+color+" align=center>" ;
      String postfix  = "</td>\n" ;
      
      if( cellInfo.getErrorCode() == 0){
      
         int red    = (int) ( ((float)precious)/((float)total) * ((float)maxPix) ) ;
         int green  = (int) ( ((float)(removable))/((float)total) * ((float)maxPix) ) ;
         int yellow = (int) ( ((float)(freespace))/((float)total) * ((float)maxPix) ) ;
         int blue   = (int) ( ((float)(total-freespace-precious-removable))/((float)total) * ((float)maxPix) ) ;

         pin( cellInfo.getCellName()+" : "+maxPix+";total="+total+";free="+freespace+";precious="+precious+";removable="+removable);
         sb.append("<tr>\n");
         sb.append(prefix).append(cellInfo.getCellName()).append(postfix) ;
         sb.append(prefix).append(cellInfo.getDomainName()).append(postfix) ;
         sb.append(prefix).append(info.getTotalSpace()/mb).append(postfix) ;
         sb.append(prefix).append(info.getFreeSpace()/mb).append(postfix) ;
         sb.append(prefix).append(info.getPreciousSpace()/mb).append(postfix) ;
	 
         sb.append("<td bgcolor="+color+" align=center>") ;
         if(red>0)sb.append("<img src=\"/images/redbox.gif\" height=10 width=").append(red).append(">") ;
         if(blue>0)sb.append("<img src=\"/images/bluebox.gif\" height=10 width=").append(blue).append(">") ;
         if(green>0)sb.append("<img src=\"/images/greenbox.gif\" height=10 width=").append(green).append(">") ;
         if(yellow>0)sb.append("<img src=\"/images/yellowbox.gif\" height=10 width=").append(yellow).append(">") ;
         sb.append("</td></tr>\n");
      }else{
         sb.append("<tr>\n");
         sb.append(prefix).append(cellInfo.getCellName()).append(postfix) ;
         sb.append(prefix).append(cellInfo.getDomainName()).append(postfix) ;
         sb.append("<td bgcolor="+errcolor+" align=center>") ;
         sb.append("<font color=red>");
         sb.append("["+cellInfo.getErrorCode()+"]") ;
         sb.append("</font></td>\n");
         sb.append("<td bgcolor="+errcolor+" align=left colspan=3>") ;
         sb.append("<font color=red>");
         sb.append(cellInfo.getErrorMessage()) ;
         sb.append("</font></td>\n");
         sb.append("</tr>\n");
      }
   }
   private int [] []  printPoolActionRow( PoolCostEntry info , ActionHeaderExtension ext , StringBuffer sb , String color ){

      String       errcolor = color ;
      
      try{
         sb.append("<tr>\n");
         sb.append("<td bgcolor="+color+" align=center>"+info.cellName+"</td>\n") ;
         sb.append("<td bgcolor="+color+" align=center>"+info.domainName+"</td>\n") ;

         int [] [] rows = info.row ;
         
         for( int j = 0 ; j < 5 ; j++ ) {
            if( rows[j] == null ){
               sb.append("<td bgcolor="+color+" align=center colspan=3>").
                 append("<font color=red>").
                 append("Integrated").
                 append("</font>").
                 append("</td>\n") ;
           
            }else{
               for( int i = 0 ; i < rows[j].length ; i++ ) {
                   sb.append("<td bgcolor="+color+" align=center>").
                      append("<font color=").
                      append(i<2?"black>":rows[j][2]>0?"red>":"#008080>").
                      append(rows[j][i]).append("</font>").
                      append("</td>\n") ;
               }
            }
         }
         if( ext != null ){
            rows = ext.getSortedMovers( info.movers ) ;
            for( int j = 0 ; j < rows.length; j++ ) {
                  for( int i = 0 ; i < rows[j].length ; i++ ) {
                      sb.append("<td bgcolor="+color+" align=center>").
                         append("<font color=").
                         append(i<2?"black>":rows[j][2]>0?"red>":"#008080>").
                         append(rows[j][i]).append("</font>").
                         append("</td>\n") ;
                  }
            }
         }
         sb.append("</tr>\n");
		 
	 return rows ;
         
      }catch( Exception e){
         esay(e);
         return null ;
      }	 
	  
   }
   /*
   private int [] []  printPoolActionRow( PoolCellInfo info , StringBuffer sb , String color ){

      String       errcolor = color ;
      
      try{
         sb.append("<tr>\n");
         sb.append("<td bgcolor="+color+" align=center>"+info.getCellName()+"</td>\n") ;
         sb.append("<td bgcolor="+color+" align=center>"+info.getDomainName()+"</td>\n") ;

         int [] [] rows = decodePoolCostInfo( info.getPoolCostInfo() ) ;
         
         for( int j = 0 ; j < 5 ; j++ ) {
            if( rows[j] == null ){
               sb.append("<td bgcolor="+color+" align=center colspan=3>").
                 append("<font color=red>").
                 append("Integrated").
                 append("</font>").
                 append("</td>\n") ;
           
            }else{
               for( int i = 0 ; i < rows[j].length ; i++ ) {
                   sb.append("<td bgcolor="+color+" align=center>").
                      append("<font color=").
                      append(i<2?"black>":rows[j][2]>0?"red>":"#008080>").
                      append(rows[j][i]).append("</font>").
                      append("</td>\n") ;
			  }
            }
         }
         
         sb.append("</tr>\n");
		 
	 return rows ;
         
      }catch( Exception e){
         esay(e);
         return null ;
      }	 
	  
   }
   */
   private void printOfflineCellInfoRow( String name , String domain , 
                                         StringBuffer sb , String bgcolor ){
//      String bgcolor = "\"#00D5FF\"" ;
      sb.append( "<tr>\n<td align=center bgcolor=").
         append(bgcolor).append(">").append(name).append("</td>\n" ) ;
      sb.append( "<td align=center bgcolor=").
         append(bgcolor).append(">").append(domain).append("</td>\n" ) ;
      sb.append( "<td align=center bgcolor=").
         append(bgcolor).
         append(" colspan=5><font color=red>OFFLINE</font></td>\n</tr>\n" ) ;

      return ;
   }
   private  void printCellInfoTable( StringBuffer sb ){
      Map  sortedMap = new TreeMap( _infoMap ) ;
      Iterator i        = sortedMap.values().iterator() ;
      long     pingTime = 0 ;
      CellInfo cellInfo = null ;
      
      printCellTableHeader( sb ) ;

      for( int n = 0 ; i.hasNext() ; n++ ){
         try{
            CellQueryInfo info = (CellQueryInfo)i.next() ;
            cellInfo = info.getCellInfo() ;
            pingTime = info.getPingTime() ;
            if( info.isOk() ){
               printCellInfoRow( cellInfo, pingTime , sb ,
                                 _rowColors[n%_rowColors.length]) ;
            }else{
               printOfflineCellInfoRow( info.getName() ,
                                        ( cellInfo == null ) ||
                                        cellInfo.getDomainName().equals("") ?
                                        "&lt;unknown&gt" : 
                                        cellInfo.getDomainName() , 
                                        sb , _rowColors[n%_rowColors.length]) ;
            }
         }catch(Exception ee){
            esay(ee);
            continue ;
         }
      }

      sb.append("</table></center>\n");
   
   }
   private synchronized void printPoolInfoTable( StringBuffer sb ){
      Map  sortedMap = new TreeMap( _infoMap ) ;
      Iterator i        = sortedMap.values().iterator() ;
      CellInfo cellInfo = null ;
      
      printPoolTableHeader( sb ) ;
      for(int n = 0 ; i.hasNext() ; n++ ){
         try{
            CellQueryInfo info = (CellQueryInfo)i.next() ;
            cellInfo      = info.getCellInfo() ;
            if(  info.isOk() && ( cellInfo instanceof PoolCellInfo ) ){
               printPoolInfoRow( (PoolCellInfo)cellInfo , sb , 
                                 _rowColors[n%_rowColors.length]) ;
            }
         }catch(Exception ee){
            esay(ee);
            continue ;
         }
      }

      sb.append("</table></center>\n");
   
   }
   private class PoolCostEntry {
       String cellName ; 
       String domainName ;
       int  [] [] row ;
       Map    movers ;
       private PoolCostEntry( String name , String domain, int [] [] row ){
           cellName    = name ;
           domainName  = domain ;
           this.row    = row ;
           this.movers = null ;
       }
       private PoolCostEntry( String name , String domain, int [] [] row ,  Map movers ){
           cellName    = name ;
           domainName  = domain ;
           this.row    = row ;
           this.movers = movers ;
       }
   }
   private synchronized List preparePoolCostTable(){
       
      List list         = new ArrayList() ;
      Map  sortedMap    = new TreeMap( _infoMap ) ;
      Iterator i        = sortedMap.values().iterator() ;
      CellInfo cellInfo = null ;
	  
      
      
      int [] [] total = new int[5][] ;
      for( int j = 0 ; j < total.length ; j++ )total[j] = new int[3] ;
      
      for(int n = 0 ; i.hasNext() ; n++ ){
         try{
            CellQueryInfo info = (CellQueryInfo)i.next() ;
	    cellInfo = info.getCellInfo() ;
            if( info.isOk() && ( cellInfo instanceof PoolCellInfo ) ){
	    
               PoolCellInfo pci = (PoolCellInfo)cellInfo ;
               int [] [] status = decodePoolCostInfo( pci.getPoolCostInfo() ) ;
               
				   
	       if( status != null )
                   list.add( new PoolCostEntry( pci.getCellName() , 
                                                pci.getDomainName() , 
                                                status ,
                                                pci.getPoolCostInfo().getExtendedMoverHash() ) ) ;
            }
         }catch(Exception ee){
            esay(ee);
            continue ;
         }
      }
      return list ;
      
   }
   private synchronized void printPoolActionTable2( StringBuffer sb ){
   
       //
       // get the translated list
       //
       dsay("Preparing pool cost table");
       List list = preparePoolCostTable() ;
       dsay("Preparing pool cost table done "+list.size());
       //
       // calculate the totals ...
       //
       Map moverMap = new HashMap() ;
       int [] [] total = new int[5][] ;
       for( int j = 0 ; j < total.length ; j++ )total[j] = new int[3] ;
       for( Iterator n = list.iterator() ; n.hasNext() ; ){
           PoolCostEntry e  = (PoolCostEntry)n.next() ;
           
           if( e.movers != null ){
              for( Iterator it = e.movers.entrySet().iterator() ; it.hasNext() ; ){
                  Map.Entry entry     = (Map.Entry)it.next() ;
                  String    queueName = (String)entry.getKey() ;
                  int [] t = (int []) moverMap.get(queueName) ;
                  if( t == null )moverMap.put( queueName , t = new int[3] ) ;
                  PoolCostInfo.PoolQueueInfo mover = (PoolCostInfo.PoolQueueInfo)entry.getValue();

                  t[0] += mover.getActive() ;
                  t[1] += mover.getMaxActive() ;
                  t[2] += mover.getQueued() ;
              }
           }
           int [] [] status = e.row ;
           for( int j = 0 ; j < total.length ; j++ ){
              for( int l = 0 ; l < total[j].length ; l++ ){
                 if( status[j] != null )total[j][l] += status[j][l] ;
              }
           }          
       }
       ActionHeaderExtension extension = new ActionHeaderExtension(moverMap) ;
       //say( "EXTENTION : "+extension);
       sb.append( "<center>\n<table border=1 cellpadding=4 cellspacing=0 width=\"90%\">\n");

       printPoolActionTableHeader( sb , extension , HEADER_TOP ) ;
       printPoolActionTableTotals( sb , extension , total ) ;
       Iterator n = list.iterator() ;
       for( int i = 1  ; n.hasNext() ; i++ ){
           PoolCostEntry e = (PoolCostEntry)n.next() ;
           printPoolActionRow( e , extension  , sb , _rowColors[i%_rowColors.length] ) ;
           if( ( _repeatHeader != 0 ) && ( i % _repeatHeader ) == 0 )printPoolActionTableHeader( sb , extension , HEADER_MIDDLE ) ; 
       }
       printPoolActionTableTotals( sb , extension , total ) ;
       printPoolActionTableHeader( sb , extension , HEADER_BOTTOM  ) ;
       sb.append("</table></center>\n");
       
       //say("Creating pool cost table ready");
   }
   private synchronized void printPoolActionTable( StringBuffer sb ){
      Map  sortedMap = new TreeMap( _infoMap ) ;
      Iterator i        = sortedMap.values().iterator() ;
      CellInfo cellInfo = null ;
	  
      
      printPoolActionTableHeader( sb , null , HEADER_TOP ) ;
      
      int [] [] total = new int[5][] ;
      for( int j = 0 ; j < total.length ; j++ )total[j] = new int[3] ;
      
      for(int n = 0 ; i.hasNext() ; n++ ){
         try{
            CellQueryInfo info = (CellQueryInfo)i.next() ;
	    cellInfo = info.getCellInfo() ;
            if( info.isOk() && ( cellInfo instanceof PoolCellInfo ) ){
	    
               PoolCellInfo pci = (PoolCellInfo) cellInfo ;
               
               int [] [] status = printPoolActionRow( 
                               new PoolCostEntry( pci.getCellName() , 
                                                  pci.getDomainName() ,
                                                  decodePoolCostInfo( pci.getPoolCostInfo() ) ) ,
			       null ,
                               sb , 
                               _rowColors[n%_rowColors.length]
			                   ) ;
				   
	       if( status != null ){
                  for( int j = 0 ; j < total.length ; j++ ){
                     for( int l = 0 ; l < total[j].length ; l++ ){
                        if( status[j] != null )total[j][l] += status[j][l] ;
                     }
                  }
               }
            }
         }catch(Exception ee){
            esay(ee);
            continue ;
         }
      }
      printPoolActionTableTotals( sb , null , total ) ;
      
      sb.append("</table></center>\n");
   
   }
   private void printPoolActionTableTotals( StringBuffer sb , ActionHeaderExtension extension , int [] [] total ){
      String bgColor = "\"#0000FF\"" ;
      String prefix  = "<td align=center bgcolor="+bgColor+"><font color=white>" ;
      String postfix = "</font></td>" ;
      
      sb.append("<tr><th colspan=2 bgcolor="+bgColor+"><font color=white>Total</font></th>") ;
      
      
      for( int j = 0 ; j < total.length ; j++ ){
         for( int l = 0 ; l < total[j].length ; l++ ){
            sb.append(prefix).append(total[j][l]).append(postfix) ; 
         }
      }
      Map map = extension == null ? null : extension.getTotals() ;
      if( map != null ){
         for( Iterator it = map.values().iterator() ; it.hasNext() ; ){
             int [] row = (int [])it.next() ;
             for( int l = 0 ; l < row.length ; l++ ){
                sb.append(prefix).append(row[l]).append(postfix) ; 
             }
         }
      }
      sb.append("</tr>") ;
       
   }
/////////////////////////////////////////////////////////////////////////////////////////////////
//
//                Prepare the info tables in the context
//
   private void preparePage(){
      try{
         //
	 // cell info tabel (request, threads , ping and creating time )
	 //
         StringBuffer sb = new StringBuffer() ;

         printEagle( sb ) ;

         sb.append("<center><table border=0 width=\"90%%\">\n").
            append("<tr><td><h1><font color=black>Services</font></h1></td></tr>\n").
            append("</table></center>\n") ;

            printCellInfoTable( sb ) ;

         sb.append("</body>\n</html>\n") ;

         _nucleus.setDomainContext( "cellInfoTable.html" , sb.toString() ) ;
         //
	 // disk usage page
	 //
         sb = new StringBuffer() ;

         printEagle( sb ) ;

         sb.append("<center><table border=0 width=\"90%%\">\n").
            append("<tr><td><h1><font color=black>Disk Space Usage</font></h1></td></tr>\n").
            append("</table></center>\n") ;

            printPoolInfoTable( sb ) ;

         sb.append("</body>\n</html>\n") ;

         _nucleus.setDomainContext( "poolUsageTable.html" , sb.toString() ) ;
         //
	 // disk usage page
	 //
         sb = new StringBuffer() ;

         printEagle( sb ) ;

         sb.append("<center><table border=0 width=\"90%%\">\n").
            append("<tr><td><h1><font color=black>Pool Request Queues</font></h1></td></tr>\n").
            append("</table></center>\n") ;
            printPoolActionTable2( sb ) ;

         sb.append("</body>\n</html>\n") ;

         _nucleus.setDomainContext( "poolQueueTable.html" , sb.toString() ) ;

      }catch(Exception ee){
         esay(ee);
         esay( "Collector reported : "+ee.getMessage());
      }
   }
   public void cleanUp(){
      say( "Clean Up sequence started" ) ;
      //
      // wait for the worker to be done
      //
      say( "Waiting for collector thread to be finished");
      _collectThread.interrupt() ;
      _senderThread.interrupt() ;
      Dictionary context = _nucleus.getDomainContext() ;
      context.remove( "cellInfoTable.html" ) ;
      
      say( "cellInfoTable.html removed from domain context" ) ;
      
      say( "Clean Up sequence done" ) ;
   
   }
   public void getInfo( PrintWriter pw ){
      pw.println("        Version : $Id: WebCollectorV3.java,v 1.24 2005-12-14 09:59:10 tigran Exp $");
      pw.println("Update Interval : "+_sleepHandler);
      pw.println("        Updates : "+_counter);
      pw.println("       Watching : "+_infoMap.size()+" cells");
      pw.println("     Debug Mode : "+(_debug?"ON":"OFF"));
      
   }
   
}
