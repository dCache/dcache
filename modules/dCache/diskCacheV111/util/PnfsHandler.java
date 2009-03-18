// $Id: PnfsHandler.java,v 1.35 2007-10-14 01:51:47 behrmann Exp $

package diskCacheV111.util ;

import java.util.List;
import java.util.Collections;

import org.apache.log4j.Logger;

import dmg.cells.nucleus.CellEndpoint;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellPath;
import dmg.cells.nucleus.NoRouteToCellException;

import diskCacheV111.vehicles.CacheStatistics;
import diskCacheV111.vehicles.PnfsAddCacheLocationMessage;
import diskCacheV111.vehicles.PnfsClearCacheLocationMessage;
import diskCacheV111.vehicles.PnfsCreateDirectoryMessage;
import diskCacheV111.vehicles.PnfsCreateEntryMessage;
import diskCacheV111.vehicles.PnfsDeleteEntryMessage;
import diskCacheV111.vehicles.PnfsFlagMessage;
import diskCacheV111.vehicles.PnfsGetCacheLocationsMessage;
import diskCacheV111.vehicles.PnfsGetCacheStatisticsMessage;
import diskCacheV111.vehicles.PnfsGetFileMetaDataMessage;
import diskCacheV111.vehicles.PnfsGetStorageInfoMessage;
import diskCacheV111.vehicles.PnfsMapPathMessage;
import diskCacheV111.vehicles.PnfsMessage;
import diskCacheV111.vehicles.PnfsRenameMessage;
import diskCacheV111.vehicles.PnfsSetFileMetaDataMessage;
import diskCacheV111.vehicles.PnfsSetLengthMessage;
import diskCacheV111.vehicles.PnfsSetStorageInfoMessage;
import diskCacheV111.vehicles.PnfsGetParentMessage;
import diskCacheV111.vehicles.PoolFileFlushedMessage;
import diskCacheV111.vehicles.StorageInfo;

import org.dcache.cells.CellMessageSender;

public class PnfsHandler
    implements CellMessageSender
{
    private final CellPath _pnfs;
    private final String _poolName;
    private long __pnfsTimeout = 30 * 60 * 1000L;
    private CellEndpoint _endpoint;

    private static final Logger _logNameSpace =
        Logger.getLogger("logger.org.dcache.namespace." + PnfsHandler.class.getName());

    @Deprecated
    public PnfsHandler(CellEndpoint endpoint,
                       CellPath pnfsManagerPath,
                       String poolName)
    {
        this(pnfsManagerPath, poolName);
        setCellEndpoint(endpoint);
    }

    @Deprecated
    public PnfsHandler(CellEndpoint endpoint,
                       CellPath pnfsManagerPath)
    {
        this(pnfsManagerPath);
        setCellEndpoint(endpoint);
    }

    public PnfsHandler(CellPath pnfsManagerPath)
    {
        this(pnfsManagerPath, "<client>");
    }

    public PnfsHandler(CellPath pnfsManagerPath,
                       String poolName)
    {
        _pnfs = pnfsManagerPath;
        _poolName = poolName;
    }

    public void setCellEndpoint(CellEndpoint endpoint)
    {
        _endpoint = endpoint;
    }

    private void send(PnfsMessage msg)
    {
        if (_endpoint == null)
            throw new IllegalStateException("Missing endpoint");

        try {
            _endpoint.sendMessage(new CellMessage(_pnfs, msg));
        } catch (NoRouteToCellException e) {
            _logNameSpace.error("Cannot send message to " + _pnfs + ": " + e);
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

   public void addCacheLocation( String pnfsId )
       throws CacheException
   {
       pnfsRequest( new PnfsAddCacheLocationMessage(
                           pnfsId,
                           _poolName)
           ) ;

   }

   public void addCacheLocation( PnfsId pnfsId )
       throws CacheException
   {
       pnfsRequest( new PnfsAddCacheLocationMessage(
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
   public List<String> getCacheLocations( PnfsId pnfsId )throws CacheException {
      PnfsGetCacheLocationsMessage pnfsMessage = new PnfsGetCacheLocationsMessage(pnfsId) ;
      pnfsMessage = pnfsRequest(pnfsMessage) ;
      List<String> assumedLocations = pnfsMessage.getCacheLocations() ;

      if (assumedLocations == null) {
          return Collections.emptyList();
      } else {
          return assumedLocations;
      }
   }

   public List<String> getCacheLocationsByPath( String fileName )throws CacheException {
      PnfsGetCacheLocationsMessage pnfsMessage = new PnfsGetCacheLocationsMessage() ;
      pnfsMessage.setPnfsPath( fileName ) ;
      pnfsMessage = pnfsRequest(pnfsMessage) ;
      List<String> assumedLocations = pnfsMessage.getCacheLocations() ;

      if (assumedLocations == null) {
          return Collections.emptyList();
      } else {
          return assumedLocations;
      }
   }

   private <T extends PnfsMessage> T pnfsRequest( T msg )
           throws CacheException {

       if (_endpoint == null)
           throw new IllegalStateException("Missing endpoint");

       T pnfsReply;
       Object      pnfsReplyObject;
       CellMessage pnfsCellReply;
       try {
           msg.setReplyRequired(true);
           pnfsCellReply
               = _endpoint.sendAndWaitToPermanent(new CellMessage(_pnfs, msg),
                                                  __pnfsTimeout);
       } catch (InterruptedException e) {
           String problem  = "PNFS handler was interrupted while waiting for a reply";
           _logNameSpace.warn(problem);
           throw new CacheException(CacheException.PANIC, problem);
       }

       if (pnfsCellReply == null) {
    	   _logNameSpace.warn("Pnfs request timed out");
    	   throw new CacheException(CacheException.TIMEOUT, "Pnfs request timed out");
       }

       pnfsReplyObject = pnfsCellReply.getMessageObject();

       /*
        * If we got a CacheException, just propagated.
        *
        * Convert Exceptions into CacheExceptions if needed.
        */
       if( pnfsReplyObject instanceof CacheException )
           throw (CacheException)pnfsReplyObject;

       if( pnfsReplyObject instanceof Throwable )
           throw new CacheException( CacheException.UNEXPECTED_SYSTEM_EXCEPTION,
               ((Throwable)pnfsReplyObject).getMessage() );

       if (!msg.getClass().equals(pnfsReplyObject.getClass())) {
    	   _logNameSpace.warn("Unexpected message arrived " + pnfsReplyObject.getClass() );
    	   throw new CacheException(CacheException.PANIC,"PANIC : Unexpected message arrived " + pnfsReplyObject.getClass());
       }

       pnfsReply = (T) pnfsReplyObject;
       if (pnfsReply.getReturnCode() != 0) {
    	   if (_logNameSpace.isDebugEnabled()) {
    		   _logNameSpace.debug("Request to PnfsManger "
                                       + msg.getClass() + " returned : "
                                       + pnfsReply.getReturnCode());
    	   }

           if (pnfsReply.getErrorObject() instanceof CacheException)
               throw (CacheException)pnfsReply.getErrorObject();

           if (pnfsReply.getErrorObject() instanceof Throwable)
               throw new CacheException( CacheException.UNEXPECTED_SYSTEM_EXCEPTION,
                       ((Throwable)pnfsReply.getErrorObject()).getMessage() );

           throw new CacheException(pnfsReply.getReturnCode(),
                  String.valueOf(pnfsReply.getErrorObject()));
       }

       return pnfsReply;
   }

   public PnfsCreateEntryMessage createPnfsDirectory( String path )
          throws CacheException                {

       return pnfsRequest(new PnfsCreateDirectoryMessage( path ) ) ;

   }
   public PnfsCreateEntryMessage createPnfsDirectory( String path , int uid , int gid , int mode )
          throws CacheException                {

       return pnfsRequest(new PnfsCreateDirectoryMessage( path , uid , gid , mode )) ;

   }

   public PnfsCreateEntryMessage createPnfsEntry( String path )
          throws CacheException                {

       return pnfsRequest(new PnfsCreateEntryMessage( path )) ;

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

       return pnfsRequest( new PnfsCreateEntryMessage( path , uid , gid , mode ) ) ;

   }

   public void setStorageInfoByPnfsId(
                PnfsId pnfsId , StorageInfo  storageInfo , int accessMode )
          throws CacheException                {

       /*
        * use request, which throws exceptions in case of.....
        */
       pnfsRequest( new PnfsSetStorageInfoMessage( pnfsId , storageInfo , accessMode ) ) ;
   }
   public PnfsGetStorageInfoMessage getStorageInfoByPnfsId( PnfsId pnfsId )
          throws CacheException                {
      return  pnfsRequest(new PnfsGetStorageInfoMessage( pnfsId.toString() )) ;

   }
   public StorageInfo getStorageInfo( String pnfsId )
          throws CacheException                {
      return pnfsRequest( new PnfsGetStorageInfoMessage( pnfsId )).getStorageInfo() ;

   }

   public PnfsGetStorageInfoMessage getStorageInfoByPath( String pnfsPath )
          throws CacheException                {
      return getStorageInfoByPath(pnfsPath, false) ;

   }

   public PnfsGetStorageInfoMessage getStorageInfoByPath( String pnfsPath ,
       boolean requestChecksum)
          throws CacheException                {

      PnfsGetStorageInfoMessage sInfo = new PnfsGetStorageInfoMessage() ;
      sInfo.setPnfsPath( pnfsPath ) ;
      sInfo.setChecksumsRequested(requestChecksum);
      return pnfsRequest( sInfo ) ;

   }
   
   public PnfsGetFileMetaDataMessage getFileMetaDataByPath( String pnfsPath )
          throws CacheException                {

      return getFileMetaDataByPath(pnfsPath, true);

   }


   public PnfsGetFileMetaDataMessage getFileMetaDataByPath( String pnfsPath , boolean followLinks)
       throws CacheException                {

        return getFileMetaDataByPath(pnfsPath, followLinks, false);
    }

      public PnfsGetFileMetaDataMessage getFileMetaDataByPath( String pnfsPath , 
          boolean followLinks, 
          boolean requestChecksum)
       throws CacheException                {

        PnfsGetFileMetaDataMessage fileMetaData = new PnfsGetFileMetaDataMessage();
        fileMetaData.setPnfsPath(pnfsPath);
        fileMetaData.setResolve(followLinks);
        fileMetaData.setChecksumsRequested(requestChecksum);
        return pnfsRequest( fileMetaData ) ;

    }


   public PnfsGetFileMetaDataMessage getFileMetaDataById( PnfsId pnfsId )
   throws CacheException                {

    PnfsGetFileMetaDataMessage fileMetaData = new PnfsGetFileMetaDataMessage(pnfsId);
    return pnfsRequest( fileMetaData ) ;
}


    public PnfsId getParentOf(PnfsId pnfsId) 
        throws CacheException
    {
            return pnfsRequest(new PnfsGetParentMessage(pnfsId)).getParent();
    }

   public void deletePnfsEntry( String path )  throws CacheException {

       PnfsDeleteEntryMessage pnfsMsg = new PnfsDeleteEntryMessage(path);
       pnfsMsg.setReplyRequired(true);

       /*
        * use request, which throws exceptions in case of.....
        */
       pnfsRequest( pnfsMsg ) ;
   }

   public void deletePnfsEntry( PnfsId pnfsid )  throws CacheException {

       PnfsDeleteEntryMessage pnfsMsg = new PnfsDeleteEntryMessage(pnfsid);
       pnfsMsg.setReplyRequired(true);

       /*
        * use request, which throws exceptions in case of.....
        */

       pnfsRequest( pnfsMsg ) ;
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

       return pnfsRequest( new PnfsGetCacheStatisticsMessage( pnfsId )).getCacheStatistics();
   }

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
                new PnfsFlagMessage( pnfsId ,flag , PnfsFlagMessage.FlagOperation.GET ) ;
       flagMessage.setReplyRequired( true );

       return pnfsRequest(flagMessage).getValue();
   }

   public void putPnfsFlag(PnfsId pnfsId, String flag, String value)
   {
       PnfsFlagMessage flagMessage =
                new PnfsFlagMessage( pnfsId ,flag , PnfsFlagMessage.FlagOperation.SET ) ;
       flagMessage.setReplyRequired( false );
       flagMessage.setValue(value);
       send(flagMessage);
   }

   public void fileFlushed(PnfsId pnfsId, StorageInfo storageInfo ) throws CacheException {

	   PoolFileFlushedMessage fileFlushedMessage = new PoolFileFlushedMessage(_poolName, pnfsId, storageInfo);

	   // throws exception if something goes wrong
	   pnfsRequest(fileFlushedMessage);

   }

	/**
	 * Get path corresponding to given pnfsid.
	 *
	 * @param pnfsID
	 * @return path
	 * @throws CacheException
	 */
	public String getPathByPnfsId(PnfsId pnfsID) throws CacheException {
		return pnfsRequest(new PnfsMapPathMessage(pnfsID)).getPnfsPath();
	}

	/**
	 * Get pnfsid corresponding to given path.
	 *
	 * @param path
	 * @return pnfsid
	 * @throws CacheException
	 */
	public PnfsId getPnfsIdByPath(String path) throws CacheException {
		return pnfsRequest(new PnfsMapPathMessage(path)).getPnfsId();
	}

}
