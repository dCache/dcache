package diskCacheV111.poolManager ;

import java.util.* ;
import diskCacheV111.util.* ;

public interface PoolSelectionUnit  {

   public interface SelectionLink {
      public String getName() ;
      public Iterator pools() ;
   
   }
  
   public interface SelectionPool {
      public long    getActive() ;
      public void    setActive( boolean active ) ;
      public boolean isReadOnly() ;
      public void    setReadOnly( boolean rdOnly ) ;
      public boolean isEnabled() ;
      public boolean setSerialId( long serialId ) ;
   }
   public void dumpSetup( StringBuffer setup ) throws Exception ;
   public SelectionPool getPool( String poolName ) ;
   public SelectionPool getPool( String poolName , boolean create ) ;
   public PoolPreferenceLevel [] 
            match( String type ,
                   String store , String dcache , String net , String protocol,
                   Map    variableMap ) ;
   public String [] getActivePools() ;
   public String [] getDefinedPools( boolean enabledOnly ) ;
   public String    getVersion() ;
   public String getNetIdentifier( String address ) ;
   public String getProtocolUnit( String protocolUnitName ) ;
   public void clear() ;
}
