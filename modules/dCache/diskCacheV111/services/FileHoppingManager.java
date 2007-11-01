// $Id: FileHoppingManager.java,v 1.3.2.1 2007-06-17 22:33:30 patrick Exp $Cg

package  diskCacheV111.services ;

import java.util.* ;
import java.text.* ;
import java.io.* ;
import java.util.regex.Pattern ;

import dmg.cells.nucleus.* ;
import dmg.util.* ;

import diskCacheV111.poolManager.* ;
import diskCacheV111.vehicles.* ;
import diskCacheV111.util.* ;

/**
  *  The FileHoppingManager receives PoolMgrReplicateFileMsg messages from 
  *  write pools and, depending on hopping rules, forward the message to 
  *  the PoolManager
  *
  *  @author Patrick Fuhrmann 
  *
  * 
  */
public class FileHoppingManager extends CellAdapter {

   private final static int REP_MESSAGE_IDLE              = 0 ;
   private final static int REP_MESSAGE_WAITING_FOR_REPLY = 1 ;
   private final static int REP_MESSAGE_CHECK_REPLICATION = 2 ;
   private final static int REP_MESSAGE_REPARE_DONE       = 3 ;

   private CellNucleus	_nucleus       = null ;
   private Args         _args          = null ;
   private TreeMap      _map           = new TreeMap() ;
   private Object       _mapLock       = new Object() ;
   private int          _totalRequests = 0 ;
   private File	        _configFile    = null ;
   private Timer         tick          = new Timer();
   private long         _retryPeriod   = 5000L ;
   private boolean      _dontQueue     = false ;
   
   private CellPath     _defaultDestinationPath = new CellPath("PoolManager");
   private MessageQueue _messageQueue           = new MessageQueue();
   
   private String _pnfsManagerName    = "PnfsManager" ;
   private long   _pnfsManagerTimeout = 20000L ;
   
   
   public  CellVersion getCellVersion(){ return new CellVersion(diskCacheV111.util.Version.getVersion(),"$Revision: 1.3.2.1 $" ); }
   
   public FileHoppingManager( String name , String args )throws Exception {
   
      super( name , FileHoppingManager.class.getName(), args , false );
      
      _args    = getArgs() ;
      _nucleus = getNucleus() ;

      try{
      
         if( _args.argc() < 1 )
            throw new
            IllegalArgumentException("Usage : <configFileName> [-useQueue[= true | false]] ");
         
         _configFile = new File( _args.argv(0) ) ;
         
         if( ! _configFile.exists() ){
            File configDir = _configFile.getParentFile() ;
            if( ( configDir == null ) || ! configDir.exists() )
              throw new
              IllegalArgumentException("Config directory doesn't exit : "+configDir);
              
            try{
               if( ! _configFile.createNewFile() )
                  throw new
                  IllegalArgumentException("Couldn't create config file : "+_configFile);
            }catch(Exception ee){
               throw new
               IllegalArgumentException("Couldn't create config file : "+_configFile+" : "+ee.getMessage());
            }
         }
         
         String optString = _args.getOpt("useQueue") ;
         _dontQueue = ( optString == null ) ||  optString.equals("false") || optString.equals("no" ) ;

      }catch(Exception ee){
         ee.printStackTrace();
         start();
         kill() ;
         throw ee ;
      }
      
      
      runSetupFile( _configFile ) ;
      
      _nucleus.export();
      tick.scheduleAtFixedRate(new QueueCheckTask() ,1000, _retryPeriod);
      
      start() ;

   }
   private void runSetupFile( File setupFile ) throws Exception {

      if( ! setupFile.exists() )
         throw new
         IllegalArgumentException( "Setup File not found : "+setupFile ) ;

      BufferedReader reader = new BufferedReader( new FileReader( setupFile ) ) ;
      try{


         String line = null ;
         while( ( line = reader.readLine() ) != null ){
            if( line.length() == 0 )continue ;
            if( line.charAt(0) == '#' )continue ;
            try{
               say( "Executing : "+line ) ;
               String answer = command( line ) ;
               if( answer.length() > 0 )say( "Answer    : "+answer ) ;
            }catch( Exception ee ){
               esay("Exception : "+ee.toString() ) ;
            }
         }
      }finally{
         try{ reader.close() ; }catch(Exception ee){}
      }

   }
   private void dumpSetup() throws Exception {
      
       File setupFile = _configFile.getCanonicalFile() ;
       File tmpFile   = new File( setupFile.getParent() , "."+setupFile.getName() ) ;
         
       PrintWriter writer =
          new PrintWriter( new FileWriter( tmpFile ) ) ;
          
       try{
          writer.print( "#\n# Setup of " ) ;
          writer.print(_nucleus.getCellName() ) ;
          writer.print(" (") ;
          writer.print(this.getClass().getName()) ;
          writer.print(") at ") ;
          writer.println( new Date().toString() ) ;
          writer.println( "#") ;

          synchronized( _mapLock ){
              for( Iterator i = _map.values().iterator() ; i.hasNext() ; ){
                  Entry e = (Entry)i.next() ;
                  writer.println( e.toCommandString() ) ;
              }
          }

          writer.println( "#" ) ;

       }catch(Exception ee){
          tmpFile.delete() ;
          throw ee ;
       }finally{
          try{ writer.close() ; }catch(Exception eee ){}
       }
       if( ! tmpFile.renameTo( setupFile ) ){
       
          tmpFile.delete() ;
          
          throw new
          IllegalArgumentException( "Rename failed : "+_configFile ) ;
           
       }
       return ;
   }
   public void getInfo( PrintWriter pw ){
       pw.println("      Cell Name : "+getCellName());
       pw.println("     Cell Class : "+this.getClass().getName() );
       pw.println("        Version : $Id: FileHoppingManager.java,v 1.3.2.1 2007-06-17 22:33:30 patrick Exp $");
       pw.println(" Total Requests : "+_totalRequests ) ;
       pw.println("Number of Rules : "+_map.size());
       pw.println("   Retry Period : "+_retryPeriod);
       pw.println("          Queue : "+ ( _dontQueue ? "Disabled" : ( "Size = "+_messageQueue.size() ) ) ) ;
       
       
   }

   /**
	* Returns the time to wait between each scan of the list in millisecond
	*/
   public long getRetryPeriod(){
	 return _retryPeriod;
          
   }
   
   /**
	* Sets the time to wait between each scan of the list in millisecond
	*/
   public void setRetryPeriod(long r){
	 _retryPeriod=r;
   }


   /**
	* Describes a rule for the hopping manager
	* @author: Patrick Fuhrmann
	*/
   private class Entry {
   
      private String  _name       = null ;
      private boolean _retry      = false ;
      private boolean _continue   = false ;
      private boolean _buffering  = false ;
      
      private String  _patternStr = null ;
      private Pattern _pattern    = null ; 
      
      private int     _status     = 0 ;
      private String  _statusStr  = null ;
      
      private String   _dest      = null ;
      private CellPath _path      = null ;
      
      private String   _source    = "write" ;
      
      private ProtocolInfo _info  = null ;
      private String   _hostName  = null ;
      private String   _protType  = null ;
      private int      _protMinor = 0 ;
      private int      _protMajor = 0 ;
      
      private int      _hit       = 0 ;
      
      private Entry( String name , 
                     String patternStr ,
                     String modeString 
                    ){
                    
         _name       = name ;
         
         _patternStr = patternStr ;
         _pattern    = Pattern.compile(patternStr) ;
         
         
         _statusStr  = modeString == null ? "keep" : modeString ;
         _status     = getModeByString( _statusStr ) ;

      }
      public String toCommandString(){
         StringBuffer sb = new StringBuffer() ;
         sb.append("define hop ").append(_name).append(" \"").
            append(_patternStr).append("\" ").append(_statusStr);
         
         if( _info != null ){
            sb.append(" -host=").append(_hostName).
               append(" -protType=").append(_protType).
               append(" -protMinor=").append(_protMinor).
               append(" -protMajor=").append(_protMajor) ;
         }
         
         if( _dest != null )
           sb.append(" -destination=").append(_dest);
         
         sb.append(" -source=").append(_source) ;
         
         if( _continue )sb.append(" -continue");
         if( _retry )sb.append(" -retry");
         if( _buffering)sb.append(" -buffering");
         
         return sb.toString();
      }
      public void setContinue( boolean isContinue ){
        _continue = isContinue ;
      }
      public void setRetry( boolean isRetry ){
        _retry = isRetry ;
      }
      public void setSource( String source ){
        _source = source ;
      }
      public void setDestination( String destination ){
         if( destination == null )return ;
         _dest       = destination ;
         _path       = new CellPath( _dest ) ;      
      }
      public int getModeByString( String modeStr ){
      
          int mode = Pool2PoolTransferMsg.UNDETERMINED ;

          if( modeStr == null ){
             mode = Pool2PoolTransferMsg.UNDETERMINED ;
          }else if( modeStr.equals("precious") ){
             mode = Pool2PoolTransferMsg.PRECIOUS ;      
          }else if( modeStr.equals("cached") ){
             mode = Pool2PoolTransferMsg.CACHED ;
          }else if( modeStr.equals("keep") ){
             mode = Pool2PoolTransferMsg.UNDETERMINED ;
          }else{
             throw new
             IllegalArgumentException("Mode string : precious|cached|keep"); 
          }
          return mode ;
      }
     
      public void setProtocolInfo( String hostName , String protType , String protMajor , String protMinor ){
      
          if( ( hostName  != null ) || ( protType  != null ) || 
              ( protMinor != null ) || ( protMajor != null )    ){

              _hostName  = hostName  == null ? "localhost" : hostName ;
              _protType  = protType  == null ? "DCap" : protType ;
              _protMinor = protMinor == null ? 0 : Integer.parseInt( protMinor ) ;
              _protMajor = protMajor == null ? 3 : Integer.parseInt( protMajor ) ;
              
              _info = new DCapProtocolInfo(
                                 _protType, _protMajor, _protMajor,
                                 hostName , 
                                 0   
                        ) ;

          }
      }
      public ProtocolInfo getProtocolInfo(){ return _info ; }
      public void setBuffering( boolean b)
      {
	    _buffering = b;
      }
      public String toString(){
         return _name+"=\""+_patternStr+"\";st="+_status+
                      ";dest="+_path+
                      ";source="+_source+
                      ";info="+(_info==null?"None":_info.toString());
      }

   }
/**
 * Container class for a PoolMgrReplicateFileMsg object ;
 * Adds the State notion, Retry counter, max retry, time to live.
 *  
 *  @author Jonathan Schaeffer
 *
 *
 */
   private class ReplicationMsg extends PoolMgrReplicateFileMsg {
      /** Describes the state of the message :
	* <ul><li>0 : nothing done</li>
	* <li>1 : sent; waiting for reply</li>
	* <li>2 : got reply</li>
	* <li>3 : replication OK</li>
	*/
       private int _state = 0;
       /** Destination for the Replication Message */
       private CellPath _destinationPath = _defaultDestinationPath;
       /** Name of the pool from which the File comes */ 
       private String _srcPool;
       /** Number of retries done for the request */
       private int _retryCount = -1;
       /** Max number of retries */
       private int _maxRetry = -1;
       /** Time to live of the request in seconds */
       private long _ttl = -1;
       /** List of pools where the file is stored */
       private Vector _cacheInfo = null;
       /** Should we keep the original file or set it as Cached ? */
       private boolean _buffering = false;

       /**
	* Constructor using a PoolMgrReplicateFileMsg and the name of the source pool.
	* @param m the message from which to build the replication request
	* @param p the name of the pool on which the file has been stored
	*/
       private ReplicationMsg(PoolMgrReplicateFileMsg m, String p)
       {
	       super( m.getPnfsId(), m.getStorageInfo(), m.getProtocolInfo(), m.getFileSize());
	       _srcPool = p;
	       setDestinationFileStatus(m.getDestinationFileStatus());
       }

       /**
	* Constructor using a PoolMgrReplicateFileMsg and the name of the source pool.
	* @param m the message from which to build the replication request
	* @param p the name of the pool on which the file has been stored
	* @param b if the original file should be kept (Precious) or not (Cached)
	*/
       private ReplicationMsg(PoolMgrReplicateFileMsg m, String p, boolean b)
       {
	       super( m.getPnfsId(), m.getStorageInfo(), m.getProtocolInfo(), m.getFileSize());
	       _srcPool = p;
	       _buffering = b;
	       setDestinationFileStatus(m.getDestinationFileStatus());
       }

       /**
	* Constructor integrating all available members
	* @param m the message from which to build the replication request
	* @param p the name of the pool on which the file has been stored
	* @param b if the original file should be kept (Precious) or not (Cached)
	* @param dest the CellPath for destination (Default : PoolManager)
	* @param r the number of current retries (Default : -1)
	* @param maxR the retry limit
	* @param ttl Time to live for this request in seconds
	*/
       private ReplicationMsg(PoolMgrReplicateFileMsg m, String p, boolean b, CellPath dest, int r, int maxR, long ttl)
       {
	       super( m.getPnfsId(), m.getStorageInfo(), m.getProtocolInfo(), m.getFileSize());
	       _srcPool         = p;
	       _buffering       = b;
	       _destinationPath = dest;
	       _retryCount      = r;
	       _maxRetry        = maxR;
	       _ttl             = ttl;
	       setReplyRequired(true);
       }

       /**
	* @return The state of the request : integer between 0 and 3 */
       public synchronized int getState(){
           return _state;
       }

       /**
	* @return the number of time the request has been sent */
       public synchronized int getRetryCount(){
           return _retryCount;
       }

       /**
	* @return the name of the pool the file comes from 
        */
       public String getSrcPool(){
           return _srcPool;
       }

       /**
	* @return the destination path of the request 
        */
       public CellPath getPath(){
           return _destinationPath;
       }

       /**
	* @return a PoolMgrReplicateFileMsg object ready to be sent 
        */
       public PoolMgrReplicateFileMsg getMessage(){
           return new PoolMgrReplicateFileMsg(getPnfsId(),getStorageInfo(), getProtocolInfo(), getFileSize());
       }

       /** Change the state of the request.
	* @param s the number for the state between 0 and 4 
	* @return true if s is a valid state
	*/
       public synchronized boolean setState(int s){
       
	   if (s <= REP_MESSAGE_REPARE_DONE){
           
		   _state = s;
		   return true;
                   
	   }
	   return false;
       }

       /** @return the correspondig description of the current state */
       public synchronized String getStringState(){
           switch( _state ){
           
              case REP_MESSAGE_IDLE              : return "Nothing Done" ;
              case REP_MESSAGE_WAITING_FOR_REPLY : return "Sent, Waiting for reply" ;
              case REP_MESSAGE_CHECK_REPLICATION : return "got Reply" ;
              case REP_MESSAGE_REPARE_DONE       : return "Replication OK" ;
              default : return "Illegal State" ;
           }
       }

       /** Increment the number _retryCount 
	* @return _retryCount
	*/
       public synchronized int incRetryCount(){
          _retryCount++;
          return _retryCount;
       }

       /**
	* @param CellPath to set as destination
	*/
       public void setDestination(CellPath p){
          _destinationPath = p;
       }

       public String toString(){
          StringBuffer sb = new StringBuffer() ;
	  sb.append( super.toString() ).
             append( "[" ).append( getStringState() ).append("]").
             append( ";Retries=").append(_retryCount) ;
	  return sb.toString();
       }

       /** Get the cacheinfo of the file from PnfsManager, using the getReplicaLocations() method */
       public void setCacheInfo(){
          _cacheInfo = getReplicaLocations(getPnfsId());
       }

       /** @return cacheinfo of the file as a vector instance */
       public Vector getCacheInfo(){
          return _cacheInfo;
       }

   }

   /** 
     * This class is a container for a list of ReplicationMsg objects.
     *
     * @author Jonathan Schaeffer
     */
   private class MessageQueue {
   
       private Map _messageQ = new HashMap() ;

       private MessageQueue(){ 
       }

       public synchronized void add(ReplicationMsg r){
          _messageQ.put( r.getPnfsId() , r );
       }

       public synchronized int size(){
          return _messageQ.size();
       }
       public synchronized Iterator iterator(){
	   return new ArrayList( _messageQ.values() ).iterator();
       }

       /** 
        * @param PnfsId : the identifier of a message
        * @return a ReplicationMsg object identified by it's pnfs file Id  from the list. Null otherwise
        */
       public synchronized ReplicationMsg getMessageByPnfsId( PnfsId pnfsid) {
       
           return (ReplicationMsg)_messageQ.get( pnfsid ) ;
           
       }	   
   }

   
   public String fh_define_hop =
      "define hop OPTIONS <name> <pattern> precious|cached|keep\n"+
      "    OPTIONS\n"+
      "       -destination=<cellDestination> # default : PoolManager\n"+
      "       -overwrite\n"+
      "       -retry\n"+
      "       -continue\n"+
      "       -buffering                     # Sets the original file as Cached when replication is done\n"+
      "       -source=write|restore|*        # !!!! for experts only\n"+
      "    StorageInfoOptions\n"+
      "       -host=<destinationHostIp>\n"+
      "       -protType=dCap|ftp...\n"+
      "       -protMinor=<minorProtocolVersion>\n"+
      "       -protMajor=<majorProtocolVersion>\n" ;
      
   public String hh_define_hop = 
      "OPTONS <name> <pattern> precious|cached|keep # see 'help define hop'" ;
   public String ac_define_hop_$_3( Args args ){
   
      String name        = args.argv(0) ;
      String patternStr  = args.argv(1) ;
      String modeStr     = args.argv(2) ;
      
      String destination = args.getOpt("destination") ;
      
      boolean overwrite  = args.getOpt("overwrite") != null ;
      
      String hostName    = args.getOpt("host") ;
      String protocol    = args.getOpt("protType") ;
      String protMinor   = args.getOpt("protMinor") ;
      String protMajor   = args.getOpt("protMajor") ;
      
      String source      = args.getOpt("source") ;
      
      Entry entry = (Entry)_map.get( name ) ;
      if( ( entry != null ) && ! overwrite )
         throw new
         IllegalArgumentException("Entry already exists : "+name ) ;
      
      entry = new Entry( name , patternStr , modeStr ) ;
      
      entry.setProtocolInfo( hostName , protocol , protMajor , protMinor ) ;
      
      entry.setDestination( destination ) ;
      
      entry.setRetry( args.getOpt("retry") != null ) ;
      entry.setContinue( args.getOpt("continue") != null ) ;
      entry.setBuffering( args.getOpt("buffering") != null ) ;
      
      if( source != null )entry.setSource(source) ;
      
      synchronized( _mapLock ){
          _map.put( name , entry ) ;
      }
      return "" ;
   }
   public String hh_rename_hop = "<oldName> <newName>" ;
   public String ac_rename_hop_$_2( Args args ){
      String oldName = args.argv(0) ;
      String newName = args.argv(1) ;
      
      synchronized( _mapLock ){
         Entry oldEntry = (Entry)_map.remove(oldName) ;
         if( oldEntry == null )
            throw new
            IllegalArgumentException("currentName not found : "+oldName );
            
         Entry newEntry = (Entry)_map.get(newName) ;
         if( newEntry != null )
            throw new
            IllegalArgumentException("newName already exists: "+newName );
      
         oldEntry._name = newName ;
         _map.put( newName , oldEntry ) ;
        
      }
      return "" ;
   }
   public String hh_undefine_hop = "<name>" ;
   public String ac_undefine_hop_$_1( Args args ){
   
      String name = args.argv(0) ;
      synchronized( _mapLock ){
          _map.remove( name ) ;
      }
      return "" ;
   }
   public void messageToForward(  CellMessage cellMessage ){
        say("Message to forward : "+cellMessage.getMessageObject().getClass().getName());
        // super.messageToForward(cellMessage);
   }

   /** Triggered when Cell gets a message and identifies the reply of a previously sent message
     *
     */
   public void messageArrived( CellMessage message ){
         
      Object   request = message.getMessageObject() ;
      CellPath srcPath = message.getSourceAddress();
	  
      
      if( request instanceof PoolMgrReplicateFileMsg ){
      
         PoolMgrReplicateFileMsg replicate = (PoolMgrReplicateFileMsg)request ;
         say("messageArrived() : from "+srcPath.getCellName() + " for file "+replicate.getPnfsId());
         
         if( replicate.isReply() ){
            checkReply(replicate);
            return ;
         }
         replicate.setReplyRequired(true);
         
         StorageInfo storageInfo = replicate.getStorageInfo() ;
         if( storageInfo == null )return ;
         
         _totalRequests ++ ;
         
         String storageClass      = storageInfo.getStorageClass()+"@"+storageInfo.getHsm() ;
         String replicationSource = storageInfo.getKey("replication.source");
         
         ProtocolInfo originalProtocolInfo = replicate.getProtocolInfo() ;
        
         int matchCount = 0 ;
         
         synchronized( _mapLock ){
         	
             for( Iterator i = _map.values().iterator() ; i.hasNext() ; ){
             
                 Entry entry = (Entry)i.next() ;
                 
                 if( ! entry._pattern.matcher( storageClass ).matches() )continue ;

                 if( ! ( ( entry._source.equals("*")                     ) || 
                         ( entry._source.indexOf(replicationSource) > -1 )    ) )continue ;
                         
                 matchCount ++ ;
                 
                 say("Entry found for : SC=<"+storageClass+"> source="+replicationSource+" : "+entry ) ;
                 entry._hit ++ ;
                 
                 CellPath path = entry._path == null ? _defaultDestinationPath : entry._path ;

                 // This has no effect on the effective File Status 
                 
                 replicate.setDestinationFileStatus( entry.getModeByString(entry._statusStr) ) ; 
                 say("Destination file will be "+entry._statusStr +
                     " ("+entry.getModeByString(entry._statusStr)+
                     ")"+replicate.getDestinationFileStatus());

                 ProtocolInfo info = entry._info == null ? originalProtocolInfo : entry._info ;            
                 replicate.setProtocolInfo( info ) ;
                 
                 if( _dontQueue ){
                    try{
                       sendMessage( new CellMessage( (CellPath)path.clone() , replicate ) )  ;
                    }catch(Exception ee ){
                       esay("Problem : couldn't forward message to : "+entry._path+" : "+ee ) ;
                    }                
                 }else{
//                   Instead of sending a message now, append 'replicate' to the message queue
                     _messageQueue.add(new ReplicationMsg(replicate, srcPath.getCellName(), entry._buffering));
                     say ("Replication Message for file "+ replicate.getPnfsId()+ " recorded");
                 }

                 if( ! entry._continue )break ;
             }
         }
         say("Total match count for <"+storageClass+"> was "+matchCount ) ;
      }
   }

   /** 
     * When a reply is identified, set the status of the file properly
     *
     */
   private void checkReply(PoolMgrReplicateFileMsg m){
      //
      // We got a reply ...
      // Find out which message is concerned by m
      //
      ReplicationMsg rmsg = _messageQueue.getMessageByPnfsId( m.getPnfsId() );
      if(rmsg == null){
      
        say("checkReply() : No replication record found for reply : "+m.toString());
        
      }else{
      
        say("checkReply() : Message for file "+m.getPnfsId()+" is reply for "+rmsg.getPnfsId()+ " replication request");
        // Mark the message as replied
        rmsg.setState(2);
        
      }
   }
      
   /** Sends a message to poolManager and gets the cacheinfo of a file
     *
     * @param pnfsId the file to ask for
     * @return Vector containing Strings, the names of the pool having the file
     *
     */
   private Vector getReplicaLocations(PnfsId pnfsId) {
         
         //
	 // count the replicas of the file
	 // Send message to PnfsManager to get cacheinfo of this file
         //
	 PnfsGetCacheLocationsMessage cacheinfo = new PnfsGetCacheLocationsMessage(pnfsId);
	 CellMessage                  msg       = new CellMessage( new CellPath( _pnfsManagerName ) , cacheinfo ) ;
         
	 try {
         
	    msg = sendAndWait( msg, _pnfsManagerTimeout );

	    ////// This part has been copied from CleanerV2 class

	    if( msg == null )
               throw new
               Exception("PnfsManager request timed out for "+pnfsId);

	    Object obj = msg.getMessageObject() ;
	    if( ! ( obj instanceof PnfsGetCacheLocationsMessage ) ){
		  //
		  // might be a NoRouteToCellException
		  //
		  if( obj instanceof Exception )throw (Exception)obj ;

		  throw new 
                  Exception("Got unexpected reply from PnfsManager "+
			     obj.getClass().getName()+
			     " instead of PnfsGetCacheLocationsMessage");
	    }

	    PnfsGetCacheLocationsMessage pnfs = (PnfsGetCacheLocationsMessage)obj ;
	    if( pnfs == null )
                 throw new
                 Exception("No reply from PnfsManager for "+pnfsId);
                

	    if( pnfs.getReturnCode() != 0 ){
            
	       Object error = pnfs.getErrorObject() ;
	       say("Got error from PnfsManager for "+pnfsId+" ["+pnfs.getReturnCode()+"] "+(error==null?"":error.toString())) ;
	       //
	       // this could be 'file no longer in pnfs from pnfsManagerV2 which
	       // is of course not an error.
	       //
	       return null ;
	    }
	    ////// End of message to pnfsManager 

	    Vector locations = pnfs.getCacheLocations() ;
	    if( locations == null ){
	      esay("getReplicaLocations() : PnfsManager replied with 'null' getCacheInfo answer for "+pnfsId);
	      return null ; 
	    }
	    say("getReplicaLocations() : CacheInfo of "+pnfs.toString() +" : "+locations);
	    return locations;
            
	 }catch(Exception ee){
	     esay("getReplicaLocations() for "+pnfsId+" failed : "+ee.toString());
	     return null;
	 }

   }

   /** Here are the conditions to determine if the replication is successful
	* You can add the call to any functions here in.
	*  @return true if replication is successfull
	*
	*/
   private boolean checkReplication(ReplicationMsg rmsg) {
	 // Checks the replication of a file
	 rmsg.setCacheInfo();
	 boolean setPrecious;
	 if ( rmsg.getCacheInfo().size() <= 1 ){
	   // File is not correctly replicated 
	   // The concern is now ... why 
	   // Anyway, don't remove this message from messageQueue
	   say("checkReplication() : File "+rmsg.getPnfsId()+" has not been replicated yet ... Retrying");
	   // Let's give it another try
	   return false;
	 }
	 else
	 {
	   return true;
	 }
   }

   public String hh_save = "" ;
   public String ac_save( Args args ) throws Exception {
      dumpSetup() ;
      return "Data written to "+_configFile ;
   }
   public String hh_ls_hop = "[<ruleName>] -count" ;
   public String ac_ls_hop_$_0_1( Args args ){
      
      StringBuffer sb = new StringBuffer() ;
      boolean   count = ( args.getOpt("count") != null ) || ( args.getOpt("c") != null ) ;
     
      synchronized( _mapLock ){
 
         if( args.argc() == 0 ){
            if( count ){
               for( Iterator i = _map.values().iterator() ; i.hasNext() ; ){
                   Entry e = (Entry)i.next() ;
                   sb.append( Formats.field( e._name       , 15 ,  Formats.RIGHT ) ).
                      append("   ").
                      append( Formats.field( e._patternStr , 30 ,  Formats.CENTER ) ).
                      append("   ").
                      append( Formats.field( ""+e._hit     ,  7 ,  Formats.RIGHT ) ).
                      append("\n");
               }
            }else{
               for( Iterator i = _map.values().iterator() ; i.hasNext() ; ){
                   Entry e = (Entry)i.next() ;
                   sb.append( e.toCommandString() ).append("\n");
               }
            }
         }else{
            String ruleName = args.argv(0) ;
            
            Entry e  = (Entry) _map.get( ruleName ) ;
            if( e == null )
              throw new
              IllegalArgumentException("Rule not found : "+ruleName);
              
            sb.append( e.toCommandString() ).append("\n");
         }
      }
      return sb.toString();
   }

   /** One last action to do when the replication is OK;
	* For example, change the Persistency of the file on the original pool
	* @param rmsg the replication message.
	* @return true if the operation is successfull.
	*
	*/
   private boolean finalizeReplication(ReplicationMsg rmsg){
         //
	 // Set the replica file as precious on one destination pool 
         //
	 boolean setPrecious = true;
	 PoolModifyPersistencyMessage modifyPersistencyRequest = null;

	 for( Iterator l=rmsg.getCacheInfo().iterator(); l.hasNext(); ){
         
	   String str = (String)l.next();
           
	   if (str.compareTo(rmsg.getSrcPool())==0){
		 if(rmsg._buffering){ 
		 	say("finalizeReplication() : Buffering mode, set file on original pool as cached");
		 	modifyPersistencyRequest=new PoolModifyPersistencyMessage(str, rmsg.getPnfsId(), !rmsg._buffering);
		 }
		 else {
		 	say("finalizeReplication() : file is kept as Precious on "+str);
		 }
	   }
	   else {
             //
             // Test if the rule's argument is "precious"
             //
             if (rmsg.getDestinationFileStatus() == Pool2PoolTransferMsg.PRECIOUS){
             
                 say("finalizeReplication() : The replicated file has to be set as precious");
                 
                 modifyPersistencyRequest=new PoolModifyPersistencyMessage(str, rmsg.getPnfsId(), setPrecious);
                 // Other replications will be set as Cached
                 // Though this should not happen as ther should only be one replication
                 // TODO : Maybe add a functionality to the hopping manager to replicate several time
                 setPrecious=false;	
             }
	   } 
	   try{
		 sendMessage( new CellMessage( new CellPath(str), modifyPersistencyRequest));
	   }catch(Exception ee ){
		 esay("Problem : couldn't send message to : "+str+" : "+ee ) ;
		 return false;
	   }
	 }
	 return true;
   }
   
   
   /** This class is a Timer class which checks the Message Queue and launches actions depending on a replication message state
	*  @author Jonathan Schaeffer
	*
	*/
   private class QueueCheckTask extends TimerTask {
	 /**
	  * This method sends the replication message, launches the checkings, launches the finalization operations and so on
	  */
         public void run() {
         
           ReplicationMsg rmsg = null;
           boolean        rep  = false;
           
           if (_messageQueue.size() > 0 ) say("TimerTask.run(): Scanning the "+_messageQueue.size()+" replication messages");
           
           for ( Iterator m = _messageQueue.iterator(); m.hasNext(); ){
           
	      rmsg = (ReplicationMsg)m.next();
              
	      switch (rmsg.getState()){
              
	          case REP_MESSAGE_IDLE : 
                  
                     say("\tSending replication message for file "+rmsg.getPnfsId());
                     
	             CellPath path = rmsg.getPath();
	             try{
	                sendMessage( new CellMessage( (CellPath)path.clone() , rmsg.getMessage() ) )  ;
	                rmsg.setState(1);
	             }catch(Exception ee ){
	                esay("\tProblem : couldn't forward message to : "+path.toString()+" : "+ee ) ;
	             }
	             rmsg.incRetryCount();
                     
	          break;

	          case REP_MESSAGE_WAITING_FOR_REPLY : 
                  
                     say("\tWaiting for a reply for file "+rmsg.getPnfsId());
                      
                  break;
                  case REP_MESSAGE_CHECK_REPLICATION : 
                  
                      say("\tChecking replication for file "+rmsg.getPnfsId()); 
                      
                      rep = checkReplication(rmsg);
                      
                      if ( ! rep ){
                         //
	                 // Replication failed, retrying
	                 //
                         rmsg.setState(REP_MESSAGE_IDLE);
                      }else{
                         //
	                 // Replication is OK
                         //
	                 rmsg.setState(REP_MESSAGE_REPARE_DONE);
                      }
                   break;
	           case REP_MESSAGE_REPARE_DONE : 
                   
                      say("\tReplication of file "+rmsg.getPnfsId()+" successfull. Removing replication task"); 
                      
	              // We can finalize the replication operation
	              // If needed, modify file persistency    
                      if (finalizeReplication(rmsg)) m.remove();
                      
                   break;
	      }
           }
         }
   }

   public String hh_ls_rep = "[pnfsid]";
   public String ac_ls_rep ( Args args) throws Exception 
   {
     
      StringBuffer sb = new StringBuffer() ;
     
      if( args.argc() > 0 ){
      
         String pnfsId = args.argv(0);
         
         ReplicationMsg rmsg = _messageQueue.getMessageByPnfsId( new PnfsId( pnfsId ) ) ;
         if( rmsg == null )
            throw new
            NoSuchElementException("PnfsId not found in queue : "+pnfsId ) ;
            
         sb.append(rmsg.toString() ).append('\n') ;
         
      }else{
         for( Iterator i = _messageQueue.iterator();  i.hasNext(); ){
      
            ReplicationMsg rmsg = (ReplicationMsg)i.next();
            sb.append( rmsg.toString() ).append('\n');
         }
      }	

      return sb.toString() ;
   }

   public String hh_check_rep = "";
   public String ac_check_rep (Args args) throws Exception
   {

       int repFailed = 0 , repDone = 0 ;
       
       for ( Iterator m = _messageQueue.iterator(); m.hasNext(); )
       {
            ReplicationMsg rmsg = (ReplicationMsg)m.next();	
            boolean rep =  checkReplication(rmsg);
            if ( ! rep ){
	      // Replication failed, retrying
	      say("Replication for file "+rmsg.getPnfsId()+" failed, retrying");
	      rmsg.setState(0);
              repFailed ++ ;
            }
            else {
	      // Replication is OK
	      say("Replication for file "+rmsg.getPnfsId()+" done");
	      rmsg.setState(3);
              repDone ++ ;
            }
       }
       return "Replication : Failed = "+repFailed+" ; Done = "+repDone ;
   }
   // On a regular basis, make a complete checkup of the message queue
   
   public void say( String message ){ super.say(message) ; pin(message) ; }
   public void esay(String message ){ super.esay(message) ; pin(message ) ; }
   public void esay(Exception ee ){ super.esay(ee) ; pin(ee.toString() ) ; }

}
