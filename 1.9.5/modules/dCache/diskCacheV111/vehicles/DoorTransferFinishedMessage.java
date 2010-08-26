// $Id: DoorTransferFinishedMessage.java,v 1.7 2005-06-01 06:03:54 patrick Exp $

package diskCacheV111.vehicles;
import  diskCacheV111.util.* ;

public class DoorTransferFinishedMessage extends Message {
   private ProtocolInfo _protocol    = null ;
   private StorageInfo  _info        = null ;
   private PnfsId       _pnfsId      = null ;
   private String       _poolName    = null ;
   private String       _ioQueueName = null ;
   private static final long serialVersionUID = -7563456962335030196L;
   
   public DoorTransferFinishedMessage( long id , 
                                       PnfsId pnfsId ,
                                       ProtocolInfo protocol ,
                                       StorageInfo  info          ){
                          
        setId( id ) ;             
        _protocol = protocol ;
        _info     = info ;
        _pnfsId   = pnfsId ;
   }
   public DoorTransferFinishedMessage( long id , 
                                       PnfsId pnfsId ,
                                       ProtocolInfo protocol ,
                                       StorageInfo  info      ,
                                       String       poolName    ){
                          
        setId( id ) ;             
        _protocol = protocol ;
        _info     = info ;
        _pnfsId   = pnfsId ;
        _poolName = poolName ;
   }
   /*
   public DoorTransferFinishedMessage( long id , ProtocolInfo info,
                                       int rc  , String errorMsg     ){
       setFailed( rc , errorMsg ) ;
       setId( id ) ;
       _protocol = info ;
   }
   public DoorTransferFinishedMessage( long id , ProtocolInfo info ){
       setSucceeded() ;
       setId(id) ;
       _protocol = info ;
   }
   */
   public void setIoQueueName( String ioQueueName ){
       _ioQueueName = ioQueueName ;
   }
   public String getIoQueueName(){
       return _ioQueueName ;
   }
   public ProtocolInfo getProtocolInfo(){ return _protocol ; }
   public StorageInfo  getStorageInfo(){ return _info ; }
   public PnfsId       getPnfsId(){ return _pnfsId ; }
   public String       getPoolName(){ return _poolName ; }
}

 
