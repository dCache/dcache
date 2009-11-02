// $Id: PoolMgrQueryPoolsMsg.java,v 1.6 2007-07-23 10:49:09 behrmann Exp $

package diskCacheV111.vehicles ;

import diskCacheV111.poolManager.PoolSelectionUnit.DirectionType;
import  java.util.*;

public class PoolMgrQueryPoolsMsg extends Message {
   private final DirectionType _accessType  ;

   private String _netUnitName      = null ;
   private String _protocolUnitName = null ;
   private StorageInfo _storageInfo = null ;
   private List<String> []_poolList         = null ;

   private static final long serialVersionUID = 4739697573589962019L;

    public PoolMgrQueryPoolsMsg(DirectionType accessType,
            String protocolUnit,
            String netUnitName,
            StorageInfo storageInfo) {
       _accessType       = accessType ;
       _protocolUnitName = protocolUnit;
       _netUnitName      = netUnitName ;
       _storageInfo      = storageInfo ;

	setReplyRequired(true);
        assert _storageInfo != null;
   }
   public DirectionType getAccessType(){ return _accessType ; }

   public String getNetUnitName(){ return _netUnitName ; }
   public String getProtocolUnitName(){ return _protocolUnitName; }
   public StorageInfo getStorageInfo(){ return _storageInfo ; }
   public void setPoolList( List<String> [] poolList ){ _poolList = poolList ; }
   public List<String> [] getPools(){ return _poolList ; }
}
