// $Id: TransferObserverV1.java,v 1.9 2005-12-14 09:59:10 tigran Exp $

package diskCacheV111.cells ;

import java.util.* ;
import java.io.* ;
import java.text.* ;
import java.lang.reflect.* ;

import dmg.util.* ;
import dmg.cells.nucleus.* ;
import dmg.cells.services.login.*;

import diskCacheV111.vehicles.* ;
import diskCacheV111.util.* ;

public class TransferObserverV1 
       extends CellAdapter 
       implements Runnable      {

    private CellNucleus _nucleus     = null ;
    private Args        _args        = null ;
    private DoorHandler _doors       = null  ;
    private String      _loginBroker = null ;
    private long        _timeout     = 30000L ;
    private Object      _resultLock  = new Object() ;
    private List        _ioList      = null ;
    private long        _update      = 120000L ;
    private long        _timeUsed    = 0L ;
    private long        _processCounter = 0 ;
    private FieldMap    _fieldMap    = null ;
    private Object      _tableLock   = new Object() ;
    private HashMap     _tableHash   = new HashMap();
    private static List __listHeader = null ;
    
    private class FieldMap {
    
        private Class       _mapClass     = null ;
        private Constructor _constructor  = null ;
        private Class   []  _conArgsClass = { dmg.util.Args.class } ;
        private Object      _master       = null ;
        private Method      _mapOwner     = null ;
        private FieldMap( String className , Args args  ) {
           if( className == null ){
              esay("FieldMap : 'fieldMap' not defined");
              return ;
           }
           Args  [] conArgs   = { args } ;
           Class [] classArgs = { java.lang.String.class } ;
           try{
              _mapClass     = Class.forName(className) ;
              _constructor  = _mapClass.getConstructor(_conArgsClass) ;
              _master       = _constructor.newInstance(conArgs); 
              _mapOwner     = _mapClass.getMethod("mapOwner", classArgs ) ;
              //
              // only if this is ok, we can load the commandlistener ...
              //
              addCommandListener( _master ) ;
              //
              say( "FieldMap : "+_mapClass.getName()+" loaded" ) ;
           }catch(Exception ee ){
              esay( "FieldMap : Creating map class Failed : "+ee ) ;
              return ;
           }
        }
        private String mapOwner( String owner ){
           Object [] args = { owner } ;
           if( _mapOwner == null )return owner ;
           try{
              return (String)_mapOwner.invoke( _master , args ) ;
           }catch(Exception ee ){
              esay("Problem invoking 'mapOwner' : "+ee ) ;
              return owner ;
           }
        }
    }
    private class DoorHandler {
       private HashMap _doors = new HashMap() ;
       private synchronized Entry defineDoor( String doorName ){
           Entry entry = (Entry) _doors.get(doorName) ;
           if( entry == null )
              _doors.put(doorName, entry = new Entry(doorName,true));
           return entry ;
       }
       private Iterator doors(){
           return _doors.keySet().iterator() ;
       }
       private Iterator entries(){
           return _doors.values().iterator() ;
       }
       private synchronized Entry undefineDoor( String doorName ){
           Entry entry = (Entry)_doors.get(doorName);
           if( entry != null )entry.setFixed(false);
           return entry ;
       }
       private synchronized Entry addDoor( String doorName ){
           Entry entry = (Entry) _doors.get(doorName) ;
           if( entry == null )
              _doors.put(doorName, entry = new Entry(doorName,false));
           return entry ;
       }
       private synchronized Entry setDoorInfo( LoginManagerChildrenInfo info ){
          String doorName = info.getCellName()+"@"+info.getCellDomainName() ;
          Entry entry = (Entry)_doors.get(doorName) ;
          if( entry == null )_doors.put(doorName,entry = new Entry( doorName ) );
          entry.setChildInfo(info);
          return entry ;
       }
       private synchronized void clear(){
          Iterator i = _doors.entrySet().iterator() ;
          while( i.hasNext() ){
             Map.Entry e = (Map.Entry)i.next() ;
             Entry entry = (Entry)e.getValue() ;
             if( entry.isFixed() )entry.setChildInfo(null);
             else i.remove() ;
          }
          return ;
       }
       private class Entry {
          private boolean _isFixed = false ;
          private String  _doorName = null ;
          private LoginManagerChildrenInfo _info = null ;
          private Entry( String doorName ){  this( doorName , false ) ; }
          private Entry( String doorName , boolean isFixed ){
             _isFixed  = isFixed ;
             _doorName = doorName ;
          }
          private LoginManagerChildrenInfo getChildInfo(){ return _info ; }
          private void setChildInfo( LoginManagerChildrenInfo info ){ _info = info ; }
          private boolean isFixed(){ return _isFixed ; }
          private void setFixed( boolean fixed ){ _isFixed = fixed ; }
       }
    
    }   
    
    public TransferObserverV1( String name , String  args ) throws Exception {
       super( name , TransferObserverV1.class.getName(), args , false ) ;
       
       _nucleus = getNucleus() ;
       _args    = getArgs() ;
       _doors   = new DoorHandler() ;
       
       try{
          if( _args.argc() < 0 )
            throw new
            IllegalArgumentException("Usage : ... ") ;
            
            //
            // check for 'doors' option. If present,
            // load them into the doors (fixed)
            //
            String doorList = _args.getOpt("doors") ;
            if( doorList != null ){
               StringTokenizer st = new StringTokenizer(doorList,",") ;
               while( st.hasMoreTokens() ){
                  _doors.defineDoor( st.nextToken() ) ;
               }
            }
            //
            try{
               String updateString = _args.getOpt("update");
               if( updateString != null )_update = Long.parseLong(updateString) * 1000L;
            }catch(Exception e){
            }
            //
            // if login broker is defined, the 
            // worker will add the 'fixed' door list to the
            // list provided by the loginBorker.
            //
            _loginBroker = _args.getOpt("loginBroker") ;
            //
            _fieldMap = new FieldMap( _args.getOpt("fieldMap") , _args ) ;
            //
            _nucleus.newThread(this,"worker").start() ;
            //
       }catch(Exception e){
          start() ;
          kill() ;
          throw e ;
       }
       useInterpreter( true ); 
       start();
       export();
    }
    private class TableEntry {
       private String  _tableName = null ;
       private int []  _fields    = null ;
       private String  _title     = null ;
       private long    _olderThan = 0L ;
       private boolean _ifNotYetStarted = false ;
       private boolean _ifMoverMissing  = false ;
       private TableEntry( String tableName , int [] fields ){
          _tableName = tableName ;
          _fields    = fields ;
       }
       private TableEntry( String tableName , int [] fields , String title ){
          _tableName = tableName ;
          _fields    = fields ;
          _title     = title ;
       }
       private int [] getFields(){ return _fields ; }
       private String getName(){ return _tableName ; }
       public String toString(){
          StringBuffer sb = new StringBuffer() ;
          sb.append(_tableName).append(" = ");
          if( _fields.length > 0 ){
             for( int i = 0 ; i < (_fields.length-1) ; i++ ){
                sb.append(Integer.toString(_fields[i])).append(",");
             }
             sb.append(Integer.toString(_fields[_fields.length-1])) ;
          }
          if( _title != null )sb.append("  \"").append(_title).append("\"");
          return sb.toString() ;
       }
       private boolean ifNotYetStarted(){ return _ifNotYetStarted ; }
       private boolean ifMoverMissing(){ return _ifMoverMissing ; }
       private long getOlderThan(){ return _olderThan ; }
    }
    public String hh_table_help = "" ;
    public String ac_table_help( Args args ){
       StringBuffer sb = new StringBuffer() ;
       Iterator     it = getFieldListHeader().iterator() ;
       for( int i = 0 ; it.hasNext() ; i++ ){
          sb.append(i).append(" ").
             append(it.next().toString()).append("\n");
       }
       return sb.toString();
    }
    public String hh_table_define = "<tableName> <n>[,<m>[,...]] [<tableHeader>]" ;
    public String ac_table_define_$_2_3( Args args ) throws Exception {
        String     tableName = args.argv(0);
        String        header = args.argc() > 2 ? args.argv(2) : null ;
        StringTokenizer st   = new StringTokenizer(args.argv(1),",");
        ArrayList       list = new ArrayList() ;
        while( st.hasMoreTokens() ){
           list.add( new Integer(st.nextToken()) ) ;
        }
        int [] array = new int[list.size()] ;
        Iterator it = list.iterator() ;
        for( int i = 0 ; it.hasNext() ; i++ ){
           array[i] = ((Integer)it.next()).intValue();
        }
        synchronized( _tableLock ){
          _tableHash.put(tableName,new TableEntry(tableName,array,header)) ;
        }
        return "";
    }
    public String hh_table_undefine = "<tableName>" ;
    public String ac_table_undefine_$_1( Args args ){
       String tableName = args.argv(0) ;
       synchronized( _tableLock ){
          _tableHash.remove( tableName );
       }
       _nucleus.getDomainContext().remove(tableName+".html") ;
       return "" ;
    }
    public String hh_table_ls = "[<tableName>]" ;
    public String ac_table_ls_$_0_1( Args args ) throws Exception {
       StringBuffer sb = new StringBuffer() ;
       if( args.argc() == 0 ){
          synchronized( _tableLock ){
             Iterator it = _tableHash.values().iterator() ;
             while( it.hasNext() ){
                sb.append( it.next().toString() ).append("\n") ;
             }
          }
          return sb.toString() ;
       }else{
          String tableName = args.argv(0) ;
          TableEntry entry = (TableEntry)_tableHash.get(tableName) ;
          if( entry == null )
            throw new
            NoSuchElementException("Not found : "+tableName) ;
          
          sb.append(entry.toString()).append("\n");
       }
       return sb.toString() ;
    }
    public String hh_set_update = "<updateTime/sec>" ;
    public String ac_set_update_$_1( Args args ){
        long update = Long.parseLong(args.argv(0)) * 1000L ;
        if( update < 10000L )
           throw new
           IllegalArgumentException("Update time must exceed 10 seconds");
           
        synchronized( this ){
           _update = update ;
           notifyAll() ;
        }
        return "Update time set to "+args.argv(0)+" seconds";
    }
    public void getInfo( PrintWriter pw ){
       pw.println( "    $Id: TransferObserverV1.java,v 1.9 2005-12-14 09:59:10 tigran Exp $" ) ;
       pw.println( "    Update Time : "+(_update/1000L)+" seconds" ) ;
       pw.println( "        Counter : "+_processCounter ) ;
       pw.println( " Last Time Used : "+_timeUsed+" msec's" ) ;
    }
    public void run(){
       while( true ){
       
           try{
           
               _processCounter ++ ;
               long start = System.currentTimeMillis() ;
               collectDataSequentially() ;
               _timeUsed = System.currentTimeMillis() - start ;
               
           }catch( Exception ee ){
           
               pin( "Exception in 'dataCollector()' : "+ee ) ;
               esay(ee);
           }
           try{
              synchronized( this ){
                 wait( _update ) ;
              }
           }catch(InterruptedException e ){
           
               esay("Data collector interrupted") ;
               break ;
           }
       }
    
    }
    //
    // lowest priority transfer observer.
    //
    private class IoEntry implements Comparable {
       private IoDoorInfo _ioDoorInfo   = null ;
       private IoDoorEntry _ioDoorEntry = null ;
       private IoJobInfo   _ioJobInfo   = null ;
       private IoEntry( IoDoorInfo info , IoDoorEntry entry ){
          _ioDoorInfo = info ;
          _ioDoorEntry = entry ;
       }
       public int compareTo( Object obj ){
          IoEntry other = (IoEntry)obj ;
          int tmp = _ioDoorInfo.getDomainName().compareTo(other._ioDoorInfo.getDomainName()) ;
          if( tmp != 0 )return tmp;
          tmp = _ioDoorInfo.getCellName().compareTo(other._ioDoorInfo.getCellName()) ;
          if( tmp != 0 )return tmp;
          return new Long( _ioDoorEntry.getSerialId() ).
                   compareTo( new Long( other._ioDoorEntry.getSerialId() ) ) ;         
       }
       public boolean equals( Object obj ){
          IoEntry other = (IoEntry)obj ;
          return _ioDoorInfo.getDomainName().equals(other._ioDoorInfo.getDomainName()) &&
                 _ioDoorInfo.getCellName().equals(other._ioDoorInfo.getCellName()) &&
                (_ioDoorEntry.getSerialId() == other._ioDoorEntry.getSerialId() ) ;
       }
    }
    public String hh_go = "[-parallel]" ;
    public String ac_go(Args args ){
      if( args.getOpt("parallel") != null ){
          _nucleus.newThread(
             new Runnable(){
                public void run(){ collectDataSequentially() ; }
             } ,
             "worker" 
          ).start() ;
          return "Started" ;
      }else{
          synchronized(this){
             notifyAll() ;
             
          }
          return "Process Notified" ;
      }
    }
    private Object request( String path , Object message )
            throws Exception {

       CellMessage request = new CellMessage(
                              new CellPath(path) ,
                              message ) ;

       request = sendAndWait( request , _timeout ) ;
       if( request == null )
          throw new 
          Exception( path+" reply timed out");

       return request.getMessageObject() ;
            
    }
    private void getBrokerInfo(){
       //
       // ask the broker for doors.
       //
       if( _loginBroker != null ){
          StringTokenizer st = new StringTokenizer(_loginBroker,",");
          ArrayList infoList = new ArrayList() ;
          
          while( st.hasMoreTokens() ){
             String loginBroker = st.nextToken() ;
             pin( "Requesting doorInfo from LoginBroker "+loginBroker ) ;
             LoginBrokerInfo [] infos = null ;
             try{
                infos = (LoginBrokerInfo [])request( loginBroker , "ls -binary" ) ;


                StringBuffer sb = new StringBuffer() ;   
                sb.append("LoginBroker ("+loginBroker+") : ") ;
                for( int i = 0 ; i < infos.length ; i++ ){
                    String doorName = 
                                    infos[i].getCellName()+"@"+
                                    infos[i].getDomainName() ;
                    _doors.addDoor( doorName ) ;
                    sb.append(doorName).append(",") ;
                }
                pin(sb.toString());
                for( int i = 0 ; i < infos.length ; i++ )infoList.add(infos[i]);
             }catch(Exception e){
                pin( "Error from sendAndWait : "+e);
             }
          }
          updateDoorPage((LoginBrokerInfo[])infoList.toArray(new LoginBrokerInfo[0])) ;
       }
    }
    
    private void collectDataSequentially(){
       _doors.clear() ;
       
       getBrokerInfo() ;
       
       pin( "Asking doors for 'doorClientList' (one by one)" ) ;
       Iterator it = _doors.doors() ;
       while( it.hasNext() ){
          String doorName = (String)it.next() ;
          pin( "Requesting client list from : "+doorName ) ;
          try{
             LoginManagerChildrenInfo info =(LoginManagerChildrenInfo)
                  request(doorName,"get children -binary") ;
                  
             pin( doorName + " reported about "+info.getChildrenCount()+
                             " children" ) ;   
                             
             _doors.setDoorInfo( info ) ;
             
          }catch(Exception e){
             pin( "Exception : "+e ) ;
          }
       }
       //
       // now we got all our Children ...
       //
       it = _doors.entries() ;
       ArrayList doorList = new ArrayList() ;
       HashMap   ioList   = new HashMap() ;
       HashSet   poolHash = new HashSet() ;
       while( it.hasNext() ){
          DoorHandler.Entry entry = (DoorHandler.Entry)it.next() ;
          LoginManagerChildrenInfo info = entry.getChildInfo() ;
          Iterator children  = info.getChildren().iterator() ;
          
          while( children.hasNext() ){
              String childDoor = children.next().toString()+"@"+
                                 info.getCellDomainName() ;

              pin( "Requesting client info from : "+childDoor ) ;
              try{

                 IoDoorInfo ioDoorInfo = (IoDoorInfo)
                      request(childDoor,"get door info -binary") ;

                 pin( childDoor + " reply ok" ) ;   

                 List ioDoorEntries = ioDoorInfo.getIoDoorEntries() ;
                 if( ioDoorEntries.size() == 0 )continue ;
                 
                 doorList.add( ioDoorInfo ) ;
                 
                 Iterator ios = ioDoorEntries.iterator() ;
                 while( ios.hasNext() ){
                    IoDoorEntry ioDoorEntry = (IoDoorEntry)ios.next() ;
                    pin( "Adding ioEntry : "+ioDoorEntry ) ;
                    ioList.put( childDoor+"#"+ioDoorEntry.getSerialId() ,
                                new IoEntry( ioDoorInfo , ioDoorEntry ) ) ;
                    String pool = ioDoorEntry.getPool() ; 
                    if( ( pool == null )  || 
                          pool.equals("") ||
                          pool.startsWith("<") )continue ;
                    poolHash.add(pool);           
                 }

              }catch(Exception e){
                 pin( "Exception : "+e ) ;
              }
                 
           }
       }
       pin( "Asking pools for io info");
       Iterator pools = poolHash.iterator() ;
       while( pools.hasNext() ){
          String poolName = pools.next().toString() ;
          pin( "Asking pool : "+poolName ) ;
          try{

             IoJobInfo [] info = (IoJobInfo [] )
                 request( poolName , "mover ls -binary" ) ;

             pin( poolName + " reply ok" ) ;   

             //
             // where is our client
             //
             for( int i = 0 ; i < info.length ; i++ ){
                 String client = info[i].getClientName()+"#"+
                                 info[i].getClientId() ;
                 IoEntry ioEntry = (IoEntry)ioList.get(client);
                 if( ioEntry == null ){
                    pin("No entry found for : "+client ) ;
                    continue ;
                 }
                 ioEntry._ioJobInfo = info[i] ;
             }


          }catch(Exception e){
             pin( "Exception : "+e ) ;
          }

       }
       List resultList = null ; 
       synchronized( _resultLock ){
          _ioList = new ArrayList( new TreeSet(ioList.values()) ) ;
          _nucleus.getDomainContext().put( "transfers.list" , _ioList ) ;
          resultList = _ioList ;
       }
       _nucleus.getDomainContext().put( "transfers.html" , createHtmlTable( resultList ) ) ;
       _nucleus.getDomainContext().put( "transfers.txt" , createAsciiTable( resultList ) ) ;
       
       createDynamicTables( resultList ) ;
       return ;
    }
    //
    // the html stuff.
    //
    private String headProlog = "<th bgcolor=\"#115259\" align=center><font color=white>" ;
    private String headEpilog = "</font></th>" ;
    private String [] prolog = {
          "<td bgcolor=\"#efefef\" align=center>" ,
          "<td bgcolor=\"#bebebe\" align=center>" 
       } ;
    private String epilog = "</td>\n" ;
    
    
    private void updateDoorPage(LoginBrokerInfo [] infos){
       StringBuffer sb = new StringBuffer() ;
       
       createHtmlHeader( sb , "Doors" ) ;
       
       sb.append("<tr>\n");
       sb.append(headProlog).append("Cell").append(headEpilog).append("\n");
       sb.append(headProlog).append("Domain").append(headEpilog).append("\n");
       sb.append(headProlog).append("Protocol").append(headEpilog).append("\n");  
       sb.append(headProlog).append("Version").append(headEpilog).append("\n");
       sb.append(headProlog).append("Host").append(headEpilog).append("\n");
       sb.append(headProlog).append("Port").append(headEpilog).append("\n");
       sb.append(headProlog).append("Load").append(headEpilog).append("\n");       
       sb.append("</tr>\n");    
       
       for( int i = 0 ; i < infos.length ; i++ ){
          int p = i % prolog.length ;
          sb.append("<tr>\n");
          sb.append(prolog[p]).append(infos[i].getCellName()).append(epilog) ;
          sb.append(prolog[p]).append(infos[i].getDomainName()).append(epilog) ;
          sb.append(prolog[p]).append(infos[i].getProtocolFamily()).append(epilog) ;
          sb.append(prolog[p]).append(infos[i].getProtocolVersion()).append(epilog) ;
          sb.append(prolog[p]).append(infos[i].getHost()).append(epilog) ;
          sb.append(prolog[p]).append(infos[i].getPort()).append(epilog) ;
          sb.append(prolog[p]).append((int)(infos[i].getLoad()*100.0)).append(epilog) ;
          sb.append("</tr>\n");
       }
       createHtmlTrailer(sb) ;
       
       _nucleus.getDomainContext().put( "doors.html" , sb.toString() ) ;
    }

    private void createDynamicTables( List list ){
       synchronized( _tableLock ){
           Iterator it = _tableHash.values().iterator() ;
           while( it.hasNext() ){
              TableEntry entry = (TableEntry)it.next() ;
              String tableName = (String)entry.getName() ;
              int [] array     = (int [])entry.getFields() ;
              _nucleus.getDomainContext().
                 put( tableName+".html" , createDynamicTable( list , array ) ) ;
           }
       }
    }
    private String createDynamicTable( List ioList , int [] fields ){
       StringBuffer sb  = new StringBuffer() ;
       Iterator     it  = ioList.iterator() ;
       long         now = System.currentTimeMillis() ;
       
       createHtmlHeader( sb ) ;
       
       createDynamicHtmlTableHeader( sb , fields ) ;
       
       for( int row = 0 ;  it.hasNext() ; row ++ ){
       
          int      p = row % prolog.length ;
          IoEntry io = (IoEntry)it.next() ;
          
          createDynamicHtmlTableRow( sb , createFieldList(io) , fields , p ) ;
       }
       createHtmlTrailer(sb) ;
       return sb.toString() ;
     
    }
    public String hh_ls_iolist = "" ;
    public String ac_ls_iolist( Args args ){
    
       List ioList = null ;
       
       synchronized( _resultLock ){ ioList = _ioList ; }
       
       if( ioList == null )return "" ;
       
       return createAsciiTable( ioList ) ;
    }
    public String createAsciiTable( List ioList ){
    
       long         now = System.currentTimeMillis() ;
       StringBuffer sb  = new StringBuffer() ;
       Iterator     it  = ioList.iterator() ;
       
       while( it.hasNext() ){
          IoEntry io = (IoEntry)it.next() ;
          sb.append(io._ioDoorInfo.getCellName()).append(" ").
             append(io._ioDoorInfo.getDomainName()).append(" ") ;
          sb.append(io._ioDoorEntry.getSerialId()).append(" ");
          sb.append(io._ioDoorInfo.getProtocolFamily()).append("-").
             append(io._ioDoorInfo.getProtocolVersion()).append(" ") ;
          sb.append(io._ioDoorInfo.getOwner()).append(" ").
             append(io._ioDoorInfo.getProcess()).append(" ") ;
          sb.append(io._ioDoorEntry.getPnfsId()).append(" ").
             append(io._ioDoorEntry.getPool()).append(" ").
             append(io._ioDoorEntry.getReplyHost()).append(" ").
             append(io._ioDoorEntry.getStatus()).append(" ").
             append((now -io._ioDoorEntry.getWaitingSince())).append(" ");
             
          if( io._ioJobInfo == null ){
             sb.append("No-Mover-Found") ;
          }else{
             sb.append(io._ioJobInfo.getStatus()).append(" ");
             if( io._ioJobInfo.getStartTime() > 0L ){
                long transferTime     = io._ioJobInfo.getTransferTime() ;
                long bytesTransferred = io._ioJobInfo.getBytesTransferred() ; 
                sb.append(transferTime).append(" ").
                   append(bytesTransferred).append(" ").
                   append( transferTime > 0 ? ( (double)bytesTransferred/(double)transferTime ) : 0 ).
                   append(" ");
                sb.append((now-io._ioJobInfo.getStartTime())).append(" ") ;
             }
          }
          sb.append("\n");
       }
       return sb.toString() ;
    }    
    public List getFieldListHeader(){
       synchronized( this.getClass() ){
          if( __listHeader == null ){
             __listHeader = new ArrayList(20) ;
             __listHeader.add("Cell") ;     //   0 
             __listHeader.add("Domain");    //   1
             __listHeader.add("Seq");       //   2
             __listHeader.add("Protocol");  //   3
             __listHeader.add("Owner");     //   4
             __listHeader.add("Process");   //   5
             __listHeader.add("PnfsId");    //   6
             __listHeader.add("Pool");      //   7
             __listHeader.add("IoHost");    //   8
             __listHeader.add("State");     //   9
             __listHeader.add("StateFor");  //  10
             __listHeader.add("JobState");  //  11
             __listHeader.add("Submitted"); //  12
             __listHeader.add("TransTime"); //  13
             __listHeader.add("TransBytes");//  14
             __listHeader.add("TransSpeed");//  15
             __listHeader.add("Started");   //  16
             
          }
          return __listHeader ;
       }
       
    }
    public List createFieldList( IoEntry io ){
    
       long         now = System.currentTimeMillis() ;
       ArrayList    out = new ArrayList(20) ;
       
       out.add(io._ioDoorInfo.getCellName()) ;
       out.add(io._ioDoorInfo.getDomainName()) ;
       out.add(Long.toString(io._ioDoorEntry.getSerialId()));
       out.add(io._ioDoorInfo.getProtocolFamily()+"-"+
               io._ioDoorInfo.getProtocolVersion()) ;
       out.add( _fieldMap.mapOwner( io._ioDoorInfo.getOwner()) ) ;
       out.add(io._ioDoorInfo.getProcess()) ;
       out.add(io._ioDoorEntry.getPnfsId().toString()) ;
       out.add(io._ioDoorEntry.getPool()) ;
       out.add(io._ioDoorEntry.getReplyHost()) ;
       out.add(io._ioDoorEntry.getStatus()) ;
       out.add( getTimeString(now -io._ioDoorEntry.getWaitingSince()) ) ;

       if( io._ioJobInfo != null ){
          out.add(io._ioJobInfo.getStatus()) ;
          out.add( getTimeString(now-io._ioJobInfo.getSubmitTime()) ) ;
          if( io._ioJobInfo.getStartTime() > 0L ){
             long transferTime     = io._ioJobInfo.getTransferTime() ;
             long bytesTransferred = io._ioJobInfo.getBytesTransferred() ; 
             out.add( getTimeString(transferTime) ) ;
             out.add( Long.toString(bytesTransferred) ) ;
             out.add( Float.toString(
                      (float)( transferTime > 0 ? 
                               ( (double)bytesTransferred/(double)transferTime ) : 
                               0.0 ) )  ) ;
             out.add( getTimeString(now-io._ioJobInfo.getStartTime())) ;
          }
       }
       return out ;
    }    
    private void createHtmlHeader( StringBuffer sb ){
       createHtmlHeader( sb , null ) ;
    }
    private void createHtmlHeader( StringBuffer sb , String title ){
       sb.append("<html>\n");
       sb.append("<head><title>Tansfer Table</title></head>\n");
       sb.append("<body background=\"/images/bg.jpg\" link=red vlink=red alink=red>\n") ;
       sb.append("<a href=\"/\"><img border=0 src=\"/images/eagleredtrans.gif\"></a>\n");
       sb.append("<br><font color=red>Birds Home</font>\n");
       sb.append("<center><img src=\"/images/eagle-grey.gif\"></center>\n");
       sb.append("<p>\n");
       sb.append("<center><table border=0 width=\"90%\">\n");
       sb.append("<tr><td><h1><font color=black>") ;
       sb.append(title == null ? "Active Transfers" : title );
       sb.append("</font></h1></td></tr>\n");
       sb.append("</table></center>\n");
       sb.append("<center>\n");
       sb.append("<table border=1 cellpadding=4 cellspacing=0 width=\"90%\">\n");
    }
    private void createHtmlTableHeader( StringBuffer sb ){
       sb.append("<tr>\n");
       sb.append(headProlog).append("Door").append(headEpilog).append("\n");
       sb.append(headProlog).append("Domain").append(headEpilog).append("\n");
       sb.append(headProlog).append("Seq").append(headEpilog).append("\n");
       
       sb.append(headProlog).append("Prot").append(headEpilog).append("\n");
       sb.append(headProlog).append("Owner").append(headEpilog).append("\n");
       sb.append(headProlog).append("Proc").append(headEpilog).append("\n");
       sb.append(headProlog).append("PnfsId").append(headEpilog).append("\n");
       sb.append(headProlog).append("Pool").append(headEpilog).append("\n");
       sb.append(headProlog).append("Host").append(headEpilog).append("\n");
       sb.append(headProlog).append("Status").append(headEpilog).append("\n");
       sb.append(headProlog).append("Wait").append(headEpilog).append("\n");
       sb.append(headProlog).append("S").append(headEpilog).append("\n");
       sb.append(headProlog).append("Bytes").append(headEpilog).append("\n");
       sb.append(headProlog).append("Speed/(k/s)").append(headEpilog).append("\n");
       
       sb.append("</tr>\n");    
    }
    private void createDynamicHtmlTableHeader( StringBuffer sb , int [] array ){
       sb.append("<tr>");
       
       List header = getFieldListHeader() ;
       
       for( int i = 0 ; i < array.length ; i++ ){
          sb.append(headProlog).
             append(header.get(array[i])).
             append(headEpilog) ;
       }
      
       sb.append("</tr>\n");   
       
       return ; 
    }
    private void createDynamicHtmlTableRow( 
          StringBuffer sb , List fieldList , int [] fields , int p ){
       sb.append("<tr>");
       int xMax = fieldList.size() ;
       for( int i = 0 ; i < fields.length ; i++ ){
          int x = fields[i] ;
          sb.append(prolog[p]).
             append(x>=xMax?"&nbsp;":fieldList.get(x)).
             append(epilog) ;
       }
      
       sb.append("</tr>\n");   
       
       return ; 
    }
    private String getTimeString( long msec ){
       int sec  =  (int) ( msec / 1000L ) ;
       int min  =  sec / 60 ; sec  = sec  % 60 ;
       int hour =  min / 60 ; min  = min  % 60 ;
       int day  = hour / 24 ; hour = hour % 24 ;
       
       String sS = Integer.toString( sec ) ;
       String mS = Integer.toString( min ) ;
       String hS = Integer.toString( hour ) ;
       
       StringBuffer sb = new StringBuffer() ;
       if( day > 0 )sb.append(day).append(" d ");
       sb.append( hS.length() < 2 ? ( "0"+hS ) : hS ).append(":");
       sb.append( mS.length() < 2 ? ( "0"+mS ) : mS ).append(":");
       sb.append( sS.length() < 2 ? ( "0"+sS ) : sS ) ;
       
       return sb.toString() ;
    }
    private void createHtmlTableRow( StringBuffer sb , IoEntry io , int p ){
       long         now = System.currentTimeMillis() ;
       sb.append("<tr>\n") ;
       sb.append(prolog[p]).append(io._ioDoorInfo.getCellName()).append(epilog);
       sb.append(prolog[p]).append(io._ioDoorInfo.getDomainName()).append(epilog) ;
       sb.append(prolog[p]).append(io._ioDoorEntry.getSerialId()).append(epilog);


       sb.append(prolog[p]).
          append(io._ioDoorInfo.getProtocolFamily()).
          append("-").
          append(io._ioDoorInfo.getProtocolVersion()).
          append(epilog);

       String tmp = io._ioDoorInfo.getOwner() ;
       tmp = tmp.indexOf("known") > -1 ? "?" : _fieldMap.mapOwner(tmp) ;
       sb.append(prolog[p]).append(tmp).append(epilog) ;

       tmp = io._ioDoorInfo.getProcess() ;
       tmp = tmp.indexOf("known") > -1 ? "?" : tmp ;
       sb.append(prolog[p]).append(tmp).append(epilog) ;

       String poolName = io._ioDoorEntry.getPool() ;
       poolName = poolName.equals("<unknown>") ? "N.N." : poolName ;
       sb.append(prolog[p]).append(io._ioDoorEntry.getPnfsId()).append(epilog) ;
       sb.append(prolog[p]).append(poolName).append(epilog) ;
       sb.append(prolog[p]).append(io._ioDoorEntry.getReplyHost()).append(epilog) ;
       sb.append(prolog[p]).append(io._ioDoorEntry.getStatus()).append(epilog) ;

       sb.append(prolog[p]).
          append( getTimeString(now -io._ioDoorEntry.getWaitingSince())).
          append(epilog);

       if( io._ioJobInfo == null ){
          if( poolName.equals("N.N.") ){
             sb.append("<td colspan=3 bgcolor=yellow align=center><font color=black>").
                append("Staging").
                append("</font></td>") ;
          }else{
             sb.append("<td colspan=3 bgcolor=red align=center><font color=white>").
                append("No Mover found").
                append("</font></td>") ;
          }
       }else{
          sb.append(prolog[p]).append(io._ioJobInfo.getStatus()).append(epilog);
          if( io._ioJobInfo.getStartTime() > 0L ){
             long transferTime     = io._ioJobInfo.getTransferTime() ;
             long bytesTransferred = io._ioJobInfo.getBytesTransferred() ; 
//                sb.append(prolog[p]).append(transferTime/1000L).append(epilog) ;
             sb.append(prolog[p]).append(bytesTransferred).append(epilog);
             sb.append(prolog[p]).
                append( transferTime > 0 ? 
                        (float)( (double)bytesTransferred/(double)transferTime ) : 
                        0.0).
                append(epilog);
//                sb.append(prolog[p]).
//                   append((now-io._ioJobInfo.getStartTime())/1000L).
//                   append(epilog) ;
          }else{
             sb.append(prolog[p]).append("-").append(epilog);
             sb.append(prolog[p]).append("-").append(epilog);
          }
       }
       sb.append("</tr>\n");
    
    }
    public String createHtmlTable( List ioList ){
    
       StringBuffer sb  = new StringBuffer() ;
       Iterator     it  = ioList.iterator() ;
       long         now = System.currentTimeMillis() ;
       
       createHtmlHeader( sb ) ;
       
       createHtmlTableHeader( sb ) ;
       
       for( int row = 0 ;  it.hasNext() ; row ++ ){
       
          int      p = row % prolog.length ;
          IoEntry io = (IoEntry)it.next() ;
          
          createHtmlTableRow( sb , io , p ) ;
       }
       createHtmlTrailer(sb) ;
       return sb.toString() ;
    }    
    private void createHtmlTrailer( StringBuffer sb ){
       sb.append("</table>\n");
       sb.append("</center><br><br><br><br><br><br><br><br><br><br><hr><br>\n");
       sb.append("<address>").append(this.getClass().getName()).append(" [$Revision: 1.9 $] at ");
       sb.append(new Date()).append("</address>\n") ;
       sb.append("</body></html>\n");
    }
    public String createErrorHtmlTable( List ioList ){
    
       StringBuffer sb  = new StringBuffer() ;
       Iterator     it  = ioList.iterator() ;
       long         now = System.currentTimeMillis() ;
       
       createHtmlHeader( sb ) ;
       
       createHtmlTableHeader( sb ) ;
       
       for( int row = 0 ;  it.hasNext() ; row ++ ){
       
          int      p = row % prolog.length ;
          IoEntry io = (IoEntry)it.next() ;
          
          if( ( io._ioJobInfo == null ) 
          
            )createHtmlTableRow( sb , io , p ) ;
       }
       createHtmlTrailer(sb) ;
       return sb.toString() ;
    }    
}
