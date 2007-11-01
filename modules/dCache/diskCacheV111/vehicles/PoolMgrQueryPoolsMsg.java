// $Id: PoolMgrQueryPoolsMsg.java,v 1.4 2006-04-20 07:40:26 patrick Exp $

package diskCacheV111.vehicles ;
import  diskCacheV111.util.* ;
import  java.util.*;
public class PoolMgrQueryPoolsMsg extends Message {
   private String _accessType       = null ;
   private String _storeUnitName    = null ;
   private String _dCacheUnitName   = null ;
   private String _netUnitName      = null ;
   private String _protocolUnitName = null ;
   private Map    _variableMap      = null ;
   private List []_poolList         = null ;
   
   private static final long serialVersionUID = 4739697573589962019L;
   
   public PoolMgrQueryPoolsMsg( String accessType ,
                                String storeUnitName ,
                                String dCacheUnitName ,
                                String netUnitName  ,
                                Map    variableMap     ){
    
       this( accessType ,
             storeUnitName ,
             dCacheUnitName ,
             null ,
             netUnitName ,
             variableMap );                            
   }
   public PoolMgrQueryPoolsMsg( String accessType ,
                                String storeUnitName ,
                                String dCacheUnitName ,
                                String protocolUnitName ,
                                String netUnitName  ,
                                Map    variableMap     ){
       _accessType       = accessType ;
       _storeUnitName    = storeUnitName ;
       _dCacheUnitName   = dCacheUnitName ;
       _protocolUnitName = protocolUnitName ;
       _netUnitName      = netUnitName ;
       _variableMap      = variableMap ;
	setReplyRequired(true);
   }
   public String getAccessType(){ return _accessType ; }
   public String getStoreUnitName(){ return _storeUnitName ; }
   public String getDCacheUnitName(){ return _dCacheUnitName ; }
   public String getNetUnitName(){ return _netUnitName ; }
   public String getProtocolUnitName(){ return _protocolUnitName; }
   public Map getVariableMap(){ return _variableMap ; }
   public void setPoolList( List [] poolList ){ _poolList = poolList ; }
   public List [] getPools(){ return _poolList ; }
}
