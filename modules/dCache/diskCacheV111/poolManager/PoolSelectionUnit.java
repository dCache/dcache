package diskCacheV111.poolManager ;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

import diskCacheV111.vehicles.StorageInfo;

public interface PoolSelectionUnit  {

   public interface SelectionLink {
      public String getName() ;
      public Iterator<SelectionPool> pools() ;
      public String  getTag() ;
   }
      
   public interface SelectionPool {
      public String  getName() ;
      public long    getActive() ;
      public void    setActive( boolean active ) ;
      public boolean isReadOnly() ;
      public void    setReadOnly( boolean rdOnly ) ;
      public boolean isEnabled() ;
      public boolean setSerialId( long serialId ) ;

      /** Returns the names of attached HSM instances. */
      public Set<String> getHsmInstances();

      /** Sets the set of names of attached HSM instances. */
      public void setHsmInstances(Set<String> hsmInstances);
   }
   
   public interface SelectionLinkGroup {
	   public String  getName() ;
	   public void add(SelectionLink link);
	   public boolean remove(SelectionLink link);
	   Collection<SelectionLink> links();
	   void attribute(String attribute, String value, boolean replace);
	   Set<String> attribute(String attribute);
	   void removeAttribute(String attribute, String value);	
	   Map<String, Set<String>> attributes();

       void setCustodialAllowed(boolean isAllowed);
       void setOutputAllowed(boolean isAllowed);
       void setReplicaAllowed(boolean isAllowed);
       void setOnlineAllowed(boolean isAllowed);
       void setNearlineAllowed(boolean isAllowed);
	   
	   public boolean isCustodialAllowed();
	   public boolean isOutputAllowed();
	   public boolean isReplicaAllowed();
	   public boolean isOnlineAllowed();
	   public boolean isNearlineAllowed();
	}
   public void dumpSetup( StringBuffer setup ) throws Exception ;
   public SelectionPool getPool( String poolName ) ;
   public SelectionPool getPool( String poolName , boolean create ) ;
   public SelectionLink getLinkByName( String linkName ) throws NoSuchElementException ;
   public PoolPreferenceLevel [] 
            match( String type ,
                   String store , String dcache , String net , String protocol,
                   StorageInfo info, String linkGroup ) ;
   public String [] getActivePools() ;
   public String [] getDefinedPools( boolean enabledOnly ) ;
   public String    getVersion() ;
   public String getNetIdentifier( String address ) ;
   public String getProtocolUnit( String protocolUnitName ) ;
   public void clear() ;
   public SelectionLinkGroup getLinkGroupByName(String linkGroupName) throws NoSuchElementException ;
   public String [] getLinkGroups();
   public String [] getLinksByGroupName(String linkGroupName) throws NoSuchElementException ;   
}
