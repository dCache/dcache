// $Id: PoolMgrQueryPoolsMsg.java,v 1.6 2007-07-23 10:49:09 behrmann Exp $

package diskCacheV111.vehicles ;

import diskCacheV111.poolManager.PoolSelectionUnit.DirectionType;
import  java.util.*;

public class PoolMgrQueryPoolsMsg extends Message {
   private final DirectionType _accessType  ;
   private String _storeUnitName    = null ;
   private String _dCacheUnitName   = null ;
   private String _netUnitName      = null ;
   private String _protocolUnitName = null ;
   private StorageInfo _storageInfo = null ;
   private List<String> []_poolList         = null ;
   
   private static final long serialVersionUID = 4739697573589962019L;
   
   public PoolMgrQueryPoolsMsg( DirectionType accessType ,
                                String storeUnitName ,
                                String dCacheUnitName ,
                                String netUnitName  ,
                                StorageInfo storageInfo     ){
    
       this( accessType ,
             storeUnitName ,
             dCacheUnitName ,
             null ,
             netUnitName ,
             storageInfo );                            
   }
   public PoolMgrQueryPoolsMsg( DirectionType accessType ,
                                String storeUnitName ,
                                String dCacheUnitName ,
                                String protocolUnitName ,
                                String netUnitName  ,
                                StorageInfo storageInfo     ){
       _accessType       = accessType ;
       _storeUnitName    = storeUnitName ;
       _dCacheUnitName   = dCacheUnitName ;
       _protocolUnitName = protocolUnitName ;
       _netUnitName      = netUnitName ;
       _storageInfo      = storageInfo ;
	setReplyRequired(true);
   }
   public DirectionType getAccessType(){ return _accessType ; }
   public String getStoreUnitName(){ return _storeUnitName ; }
   public String getDCacheUnitName(){ return _dCacheUnitName ; }
   public String getNetUnitName(){ return _netUnitName ; }
   public String getProtocolUnitName(){ return _protocolUnitName; }
   public StorageInfo getStorageInfo(){ return _storageInfo ; }
   public void setPoolList( List<String> [] poolList ){ _poolList = poolList ; }
   public List<String> [] getPools(){ return _poolList ; }
}
