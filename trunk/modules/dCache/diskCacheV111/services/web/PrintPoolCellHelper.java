 
// $Id: PrintPoolCellHelper.java,v 1.1 2006-06-05 08:51:28 patrick Exp $Cg

package  diskCacheV111.services.web ;

import   dmg.cells.nucleus.* ;
import   dmg.cells.network.* ;
import   dmg.util.* ;

import   diskCacheV111.pools.* ;

import java.util.* ;
import java.text.* ;
import java.io.* ;
import java.net.* ;

public class PrintPoolCellHelper {


   private String []  _rowColors = { "\"#efefef\"" , "\"#bebebe\"" } ;
   private SimpleDateFormat _formatter    = new SimpleDateFormat ("MM/dd HH:mm:ss");
   private int        _repeatHeader    = 30 ;
   
   public PrintPoolCellHelper(){
   
   }
   private static final int HEADER_TOP    =   0 ;
   private static final int HEADER_MIDDLE =   1 ;
   private static final int HEADER_BOTTOM =   2 ;
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
         return null ;
      }	 
	  
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
         return null ;
      }	 
   }
   public synchronized void printPoolActionTable( StringBuffer sb , Collection itemSet ){
   
       //
       // get the translated list
       //
       List list = preparePoolCostTable( itemSet ) ;
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

       sb.append( "<center>\n<table border=1 cellpadding=4 cellspacing=0 width=\"95%\">\n");

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
       
   }
   private synchronized List preparePoolCostTable( Collection itemSet ){
       
      List list         = new ArrayList() ;
      Iterator i        = itemSet.iterator() ;
      CellInfo cellInfo = null ;
	  
      
      
      int [] [] total = new int[5][] ;
      for( int j = 0 ; j < total.length ; j++ )total[j] = new int[3] ;
      
      for(int n = 0 ; i.hasNext() ; n++ ){
         try{
            PoolCellQueryContainer.PoolCellQueryInfo info = (PoolCellQueryContainer.PoolCellQueryInfo)i.next() ;            
	    cellInfo = info.getPoolCellInfo() ;
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
            continue ;
         }
      }
      return list ;
      
   }
   /*------------------------------------------------------------------------------------------------------
    *
    */
   public synchronized void printPoolInfoTable( StringBuffer sb , Collection itemSet ){
   
      CellInfo cellInfo = null ;
      
      long [] spaces = sumUpSpaces( itemSet ) ;
      
      printPoolTableHeader( sb , false ) ;
      printPoolSumInfoRow( "SUM" , "-" , spaces ,sb , "#778899" );
      
      Iterator i        = itemSet.iterator() ;
      for(int n = 0 ; i.hasNext() ; n++ ){
         try{
            PoolCellQueryContainer.PoolCellQueryInfo info = (PoolCellQueryContainer.PoolCellQueryInfo)i.next() ;            
            cellInfo      = info.getPoolCellInfo() ;
            if(  info.isOk() && ( cellInfo instanceof PoolCellInfo ) ){
               printPoolInfoRow( (PoolCellInfo)cellInfo , sb , 
                                 _rowColors[n%_rowColors.length]) ;
            }
         }catch(Exception ee){
            ee.printStackTrace();
            continue ;
         }
      }

      printPoolSumInfoRow( "SUM" , "-" , spaces ,sb , "#778899" );
      sb.append("</table></center>\n");
   
   }
   
   public long [] sumUpSpaces( Collection itemSet ){
   
      CellInfo cellInfo = null ;
      long []  result   = new long[4] ;
      
      for( Iterator i = itemSet.iterator() ; i.hasNext() ; ){
         try{
            PoolCellQueryContainer.PoolCellQueryInfo info = (PoolCellQueryContainer.PoolCellQueryInfo)i.next() ;            
            cellInfo = info.getPoolCellInfo() ;
            if(  info.isOk() && ( cellInfo instanceof PoolCellInfo ) ){
               PoolCostInfo.PoolSpaceInfo spaceInfo = ((PoolCellInfo)cellInfo).getPoolCostInfo().getSpaceInfo() ;

               result[0]     += spaceInfo.getTotalSpace() ;
               result[1]     += spaceInfo.getFreeSpace() ;
               result[2]     += spaceInfo.getPreciousSpace() ;
               result[3]     += spaceInfo.getRemovableSpace() ;
            }
         }catch(Exception ee){
            ee.printStackTrace();
            continue ;
         }
      }
      return result ;
   }   
   private void printPoolTableHeader( StringBuffer sb , boolean isGroup ){

      sb.append( "<center>\n<table border=1 cellpadding=4 cellspacing=0 width=\"95%\">\n");
      sb.append("<tr>\n");
      String color = "\"#115259\"" ;
      if( isGroup ){
         sb.append("<td bgcolor="+color+" align=center><font color=white>"+"PoolGroup"+"</font></td>\n") ;
      }else{
         sb.append("<td bgcolor="+color+" align=center><font color=white>"+"CellName"+"</font></td>\n") ;
         sb.append("<td bgcolor="+color+" align=center><font color=white>"+"DomainName"+"</font></td>\n") ;
      }
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
   private void printPoolInfoRow( PoolCellInfo cellInfo , StringBuffer sb , String color ){
      long mb     = 1024*1024 ;
      int  maxPix = 300 ;
      
      PoolCostInfo.PoolSpaceInfo info = cellInfo.getPoolCostInfo().getSpaceInfo() ;
      
      long total     = info.getTotalSpace() ;
      long freespace = info.getFreeSpace() ;
      long precious  = info.getPreciousSpace() ;
      long removable = info.getRemovableSpace() ;

      String errcolor = color ;
      String prefix   = "<td bgcolor="+color+" align=center>" ;
      String postfix  = "</td>\n" ;
      
      if( cellInfo.getErrorCode() == 0){
      
         int red    = (int) ( ((float)precious)/((float)total) * ((float)maxPix) ) ;
         int green  = (int) ( ((float)(removable))/((float)total) * ((float)maxPix) ) ;
         int yellow = (int) ( ((float)(freespace))/((float)total) * ((float)maxPix) ) ;
         int blue   = (int) ( ((float)(total-freespace-precious-removable))/((float)total) * ((float)maxPix) ) ;

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
   private void printPoolSumInfoRow( String primary , String secondary , long [] spaces , StringBuffer sb , String color ){
      long mb     = 1024*1024 ;
      int  maxPix = 300 ;
      
      long total      = spaces[0] ;
      long freespace  = spaces[1] ;
      long precious   = spaces[2] ;
      long removable  = spaces[3] ;

      String prefix   = "<td bgcolor="+color+" align=center>" ;
      String postfix  = "</td>\n" ;
      
      int red    = (int) ( ((float)precious)/((float)total) * ((float)maxPix) ) ;
      int green  = (int) ( ((float)(removable))/((float)total) * ((float)maxPix) ) ;
      int yellow = (int) ( ((float)(freespace))/((float)total) * ((float)maxPix) ) ;
      int blue   = (int) ( ((float)(total-freespace-precious-removable))/((float)total) * ((float)maxPix) ) ;

      sb.append("<tr>\n");
      if( primary != null )sb.append(prefix).append(primary).append(postfix) ;
      if( secondary != null )sb.append(prefix).append(secondary).append(postfix) ;
      sb.append(prefix).append(total/mb).append(postfix) ;
      sb.append(prefix).append(freespace/mb).append(postfix) ;
      sb.append(prefix).append(precious/mb).append(postfix) ;

      sb.append("<td bgcolor="+color+" align=center>") ;
      if(red>0)sb.append("<img src=\"/images/redbox.gif\" height=10 width=").append(red).append(">") ;
      if(blue>0)sb.append("<img src=\"/images/bluebox.gif\" height=10 width=").append(blue).append(">") ;
      if(green>0)sb.append("<img src=\"/images/greenbox.gif\" height=10 width=").append(green).append(">") ;
      if(yellow>0)sb.append("<img src=\"/images/yellowbox.gif\" height=10 width=").append(yellow).append(">") ;
      sb.append("</td></tr>\n");

   }
   /*------------------------------------------------------------------------------------------------------
    *
    */
   public  void printCellInfoTable( StringBuffer sb , Collection itemSet ){

      Iterator i        = itemSet.iterator() ;
      long     pingTime = 0 ;
      CellInfo cellInfo = null ;
      
      printCellTableHeader( sb ) ;

      for( int n = 0 ; i.hasNext() ; n++ ){
         try{
            PoolCellQueryContainer.PoolCellQueryInfo info = (PoolCellQueryContainer.PoolCellQueryInfo)i.next() ;
            cellInfo = info.getPoolCellInfo() ;
            pingTime = info.getPingTime() ;
            if( info.isOk() ){
               printCellInfoRow( cellInfo, pingTime , sb ,
                                 _rowColors[n%_rowColors.length]) ;
            }else{
               printOfflineCellInfoRow( cellInfo.getCellName() ,
                                        ( cellInfo == null ) ||
                                        cellInfo.getDomainName().equals("") ?
                                        "&lt;unknown&gt" : 
                                        cellInfo.getDomainName() , 
                                        sb , _rowColors[n%_rowColors.length]) ;
            }
         }catch(Exception ee){
            continue ;
         }
      }

      sb.append("</table></center>\n");
   
   }
   private void printOfflineCellInfoRow( String name , String domain , 
                                         StringBuffer sb , String bgcolor ){

      sb.append( "<tr>\n<td align=center bgcolor=").
         append(bgcolor).append(">").append(name).append("</td>\n" ) ;
      sb.append( "<td align=center bgcolor=").
         append(bgcolor).append(">").append(domain).append("</td>\n" ) ;
      sb.append( "<td align=center bgcolor=").
         append(bgcolor).
         append(" colspan=5><font color=red>OFFLINE</font></td>\n</tr>\n" ) ;

      return ;
   }
   private void printCellTableHeader( StringBuffer sb ){
      sb.append( "<center>\n<table border=1 cellpadding=4 cellspacing=0 width=\"95%\">\n");
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
   
}
