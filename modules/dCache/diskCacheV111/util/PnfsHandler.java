// $Id: PnfsHandler.java,v 1.29.4.2 2007-04-02 18:19:01 tigran Exp $

package diskCacheV111.util ;

import java.util.* ;
import dmg.cells.nucleus.* ;
import diskCacheV111.vehicles.* ;
public class PnfsHandler {

   private CellPath    _pnfs ;
   private CellAdapter _cell ;
   private String      _poolName ;
   private long __pnfsTimeout = 30 * 60 * 1000L ;
   public PnfsHandler( CellAdapter parent ,
                       CellPath    pnfsManagerPath ,
                       String      poolName  ){
                             
       _cell     = parent ;
       _pnfs     = pnfsManagerPath ;
       _poolName = poolName ;
                                
   }
   public PnfsHandler( CellAdapter parent ,
                       CellPath    pnfsManagerPath  ){
                             
       _cell     = parent ;
       _pnfs     = pnfsManagerPath ;
       _poolName = "<client>" ;
                                
   }
   private void send( PnfsMessage msg ){
       try {
	   _cell.sendMessage(new CellMessage( _pnfs , msg ) );
       } catch (Exception e){
	   esay("Cannot send messge to pnfs manager "+e);
           esay(e);
       }
   }
   //
   //
   public void clearCacheLocation( String pnfsId ){
       
       send( new PnfsClearCacheLocationMessage(
                           pnfsId,
                           _poolName) 
           ) ;
       
   }
   public void clearCacheLocation( PnfsId pnfsId ){
       clearCacheLocation( pnfsId , false  );
   }
   public void clearCacheLocation( PnfsId pnfsId , boolean removeIfLast  ){
       
       send( new PnfsClearCacheLocationMessage(
                           pnfsId,
                           _poolName,
                           removeIfLast) 
           ) ;
       
   }
   public void clearCacheLocation( PnfsId pnfsId , String poolName ){
       
       send( new PnfsClearCacheLocationMessage(
                           pnfsId,
                           poolName) 
           ) ;
       
   }

   public void addCacheLocation( String pnfsId ){
       
       send( new PnfsAddCacheLocationMessage(
                           pnfsId,
                           _poolName) 
           ) ;
       
   }
   public void addCacheLocation( PnfsId pnfsId ){
       
       send( new PnfsAddCacheLocationMessage(
                           pnfsId,
                           _poolName) 
           ) ;
       
   }
   public void setFileSize( String pnfsId , long length )throws CacheException {
       
       pnfsRequest( new PnfsSetLengthMessage(
                           new PnfsId(pnfsId),
                           length  ) 
           ) ;
       
   }
   public void setFileSize( PnfsId pnfsId , long length )throws CacheException {
       
       pnfsRequest( new PnfsSetLengthMessage(
                           pnfsId,
                           length  ) 
           ) ;
       
   }
   public List getCacheLocations( PnfsId pnfsId )throws CacheException {
      PnfsGetCacheLocationsMessage pnfsMessage = new PnfsGetCacheLocationsMessage(pnfsId) ;
      pnfsMessage = (PnfsGetCacheLocationsMessage)pnfsRequest(pnfsMessage) ;
      List assumedLocations = pnfsMessage.getCacheLocations() ;
      
      return assumedLocations == null ? new Vector() : assumedLocations ;
   
   }

   public List getCacheLocationsByPath( String fileName )throws CacheException {
      PnfsGetCacheLocationsMessage pnfsMessage = new PnfsGetCacheLocationsMessage() ;
      pnfsMessage.setPnfsPath( fileName ) ;
      pnfsMessage = (PnfsGetCacheLocationsMessage)pnfsRequest(pnfsMessage) ;
      List assumedLocations = pnfsMessage.getCacheLocations() ;
      
      return assumedLocations == null ? new Vector() : assumedLocations ;
   
   }
   
   private PnfsMessage pnfsRequest( PnfsMessage msg )
           throws CacheException {
           
       PnfsMessage pnfsReply = null ;
       Object      pnfsReplyObject;
       CellMessage pnfsCellReply ;
       try {
           pnfsCellReply
	         = _cell.sendAndWait( 
                      new CellMessage( _pnfs , msg) ,
                      __pnfsTimeout
                                    ) ;
                                
       }catch (Exception e){
           String problem  = "Exception sending pnfs request : "+ e ;
           esay( problem ) ;
           esay(e) ;
	   throw new
           CacheException( 115 , problem ) ;
       }
       try{
	   if( pnfsCellReply == null )
	      throw new
              CacheException( CacheException.TIMEOUT , "Pnfs request timed out" ) ;
           
	   pnfsReplyObject = pnfsCellReply.getMessageObject();

	   if( ! msg.getClass().equals( pnfsReplyObject.getClass() ) )
	       throw new
               CacheException( CacheException.PANIC , 
                               "PANIC : Unexpected message arrived "+
                               pnfsReplyObject.getClass() ) ;
       }catch (CacheException ce){
           esay( "CacheException ("+ce.getRc()+") : "+ce.getMessage() ) ;
           throw ce ;
       }
                               
       pnfsReply = (PnfsMessage)pnfsReplyObject;
       if( pnfsReply.getReturnCode() != 0 )
           throw new
               CacheException( pnfsReply.getReturnCode() ,
                               "Pnfs error : "+
                               pnfsReply.getErrorObject() );
       return pnfsReply ;
   }
   public Message messageRequest( CellPath path , Message msg )
           throws CacheException {
           
       Message     reply = null ;
       Object      replyObject;
       CellMessage cellReply ;
       try {
           cellReply
	         = _cell.sendAndWait( 
                      new CellMessage( path , msg) ,
                      __pnfsTimeout
                                    ) ;
                                
       }catch (Exception e){
           String problem  = "Exception sending request : "+ e ;
           esay( problem ) ;
           esay(e) ;
	   throw new
           CacheException( 115 , problem ) ;
       }
       try{
	   if( cellReply == null )
	      throw new
              CacheException( 112 , "Request timed out" ) ;
           
	   replyObject = cellReply.getMessageObject();

	   if( ! msg.getClass().equals( replyObject.getClass() ) )
	       throw new
               CacheException( CacheException.PANIC , 
                               "PANIC : Unexpected message arrived "+
                               replyObject.getClass() ) ;
                               
	   reply = (Message)replyObject;
	   if( reply.getReturnCode() != 0 )
	       throw new
               CacheException( 114 ,
                               "Request Error : "+
                               reply.getErrorObject() );
	   

       }catch (CacheException ce){
           esay( "CacheException ("+ce.getRc()+") : "+ce.getMessage() ) ;
           throw ce ;
       }
       return reply ;
   }
   
   public PnfsCreateEntryMessage createPnfsDirectory( String path )
          throws CacheException                {
          
       return (PnfsCreateEntryMessage)pnfsRequest( 
                  new PnfsCreateDirectoryMessage( path ) 
                          ) ;

   }
   public PnfsCreateEntryMessage createPnfsDirectory( String path , int uid , int gid , int mode )
          throws CacheException                {
          
       return (PnfsCreateEntryMessage)pnfsRequest( 
                  new PnfsCreateDirectoryMessage( path , uid , gid , mode ) 
                          ) ;

   }
   
   public PnfsCreateEntryMessage createPnfsEntry( String path )
          throws CacheException                {
          
       return (PnfsCreateEntryMessage)pnfsRequest( 
                  new PnfsCreateEntryMessage( path ) 
                          ) ;

   }
   
   
   public void pnfsSetFileMetaData( PnfsId pnfsId, FileMetaData meta) {
       PnfsSetFileMetaDataMessage msg =  new PnfsSetFileMetaDataMessage( pnfsId );
       msg.setMetaData( meta );       
       send(msg );
       return;
   }
   

   public void renameEntry( PnfsId pnfsId, String newName )  throws CacheException {
       
       PnfsRenameMessage pnfsMsg = new PnfsRenameMessage(pnfsId, newName);
       pnfsMsg.setReplyRequired(true);
       
       PnfsMessage msg = pnfsRequest( pnfsMsg ) ;
       if ( msg.getReturnCode() != 0 ) {
           throw new CacheException( msg.getErrorObject().toString() );
       }
   }
   
   
   public PnfsCreateEntryMessage createPnfsEntry( String path , int uid , int gid , int mode )
          throws CacheException                {
          
       return (PnfsCreateEntryMessage)pnfsRequest( 
                  new PnfsCreateEntryMessage( path , uid , gid , mode ) 
                          ) ;

   }
   
   public void setStorageInfoByPnfsId( 
                PnfsId pnfsId , StorageInfo  storageInfo , int accessMode )
          throws CacheException                {
          
       Message message =
       (Message)pnfsRequest( 
                    new PnfsSetStorageInfoMessage( pnfsId , storageInfo , accessMode ) 
                          ) ;
                          
       if( message.getReturnCode() != 0 )
          throw new
          CacheException( message.getReturnCode() , message.getErrorObject().toString() ) ;

   }
   public PnfsGetStorageInfoMessage getStorageInfoByPnfsId( PnfsId pnfsId )
          throws CacheException                {
      return 
          ((PnfsGetStorageInfoMessage)(
              pnfsRequest( 
                  new PnfsGetStorageInfoMessage( pnfsId.toString() ) 
                          ))) ;

   }
   public StorageInfo getStorageInfo( String pnfsId )
          throws CacheException                {
      return 
          ((PnfsGetStorageInfoMessage)(
              pnfsRequest( 
                  new PnfsGetStorageInfoMessage( pnfsId ) 
                          ))).getStorageInfo() ;

   }
   
   public PnfsGetStorageInfoMessage getStorageInfoByPath( String pnfsPath )
          throws CacheException                {
	  
      PnfsGetStorageInfoMessage sInfo = new PnfsGetStorageInfoMessage() ;
      sInfo.setPnfsPath( pnfsPath ) ;
      return (PnfsGetStorageInfoMessage)pnfsRequest( sInfo ) ;

   }
   
   public PnfsGetFileMetaDataMessage getFileMetaDataByPath( String pnfsPath )
          throws CacheException                {

      return getFileMetaDataByPath(pnfsPath, true);

   }


   public PnfsGetFileMetaDataMessage getFileMetaDataByPath( String pnfsPath , boolean followLinks)
       throws CacheException                {
        
        PnfsGetFileMetaDataMessage fileMetaData = new PnfsGetFileMetaDataMessage();
        fileMetaData.setPnfsPath(pnfsPath);
        fileMetaData.setResolve(followLinks);
        return (PnfsGetFileMetaDataMessage)pnfsRequest( fileMetaData ) ;
    
    }

   
   public PnfsGetFileMetaDataMessage getFileMetaDataById( PnfsId pnfsId )
   throws CacheException                {
    
    PnfsGetFileMetaDataMessage fileMetaData = new PnfsGetFileMetaDataMessage(pnfsId);
    return (PnfsGetFileMetaDataMessage)pnfsRequest( fileMetaData ) ;
}
   
   
   public void deletePnfsEntry( String path )  throws CacheException {
       
       PnfsDeleteEntryMessage pnfsMsg = new PnfsDeleteEntryMessage(path);
       pnfsMsg.setReplyRequired(true);
       
       PnfsMessage msg = pnfsRequest( pnfsMsg ) ;
       if ( msg.getReturnCode() != 0 ) {
           throw new CacheException( msg.getErrorObject().toString() );
       }
   }

   public void deletePnfsEntry( PnfsId pnfsid )  throws CacheException {
       
       PnfsDeleteEntryMessage pnfsMsg = new PnfsDeleteEntryMessage(pnfsid);
       pnfsMsg.setReplyRequired(true);
       
       PnfsMessage msg = pnfsRequest( pnfsMsg ) ;
       if ( msg.getReturnCode() != 0 ) {
           throw new CacheException( msg.getErrorObject().toString() );
       }
   }

   
   
   public void renameEntryToUnique(PnfsId pnfsId) {
       
       PnfsRenameMessage pnfsMsg = new PnfsRenameMessage(pnfsId, true);       
       pnfsMsg.setReplyRequired(false);
       
       try {
           PnfsMessage msg = pnfsRequest( pnfsMsg ) ;
           send(msg);
       }catch(Exception ingnored) {}
   }
   
   
   public CacheStatistics getCacheStatistics( String pnfsId ) {
      try{
         return _getCacheStatistics( pnfsId ) ;
      }catch(CacheException ce ){
         return new CacheStatistics( pnfsId ) ;
      }
   }
   public CacheStatistics _getCacheStatistics( String pnfsId ) 
          throws CacheException {
   
       return 
          ((PnfsGetCacheStatisticsMessage)(
               pnfsRequest(
                  new PnfsGetCacheStatisticsMessage( pnfsId )
                          )               )).getCacheStatistics();
   }
   private void say( String str ){ _cell.say( "PnfsHandler : "+str ) ; } 
   private void esay( String str ){ _cell.esay( "PnfsHandler : "+str ) ; }
   private void esay(Throwable t ){ _cell.esay(t) ; } 

   /**
    * Getter for property __pnfsTimeout.
    * @return Value of property __pnfsTimeout.
    */
   public long getPnfsTimeout() {
       return __pnfsTimeout;
   }
   
   /**
    * Setter for property __pnfsTimeout.
    * @param __pnfsTimeout New value of property __pnfsTimeout.
    */
   public void setPnfsTimeout(long __pnfsTimeout) {
       this.__pnfsTimeout = __pnfsTimeout;
   }
   
   public String getPnfsFlag(PnfsId pnfsId, String flag) 
   throws CacheException
   {
       PnfsFlagMessage flagMessage =
                new PnfsFlagMessage( pnfsId ,flag , "get" ) ;
       flagMessage.setReplyRequired( true );
       flagMessage = (PnfsFlagMessage)pnfsRequest(flagMessage);
       return flagMessage.getValue();
       
   }
   
   public void putPnfsFlag(PnfsId pnfsId, String flag, String value) 
   throws CacheException
   {
       PnfsFlagMessage flagMessage =
                new PnfsFlagMessage( pnfsId ,flag , "put" ) ;
       flagMessage.setReplyRequired( false );
       flagMessage.setValue(value);
       send(flagMessage);
   }
   
   
   
}
