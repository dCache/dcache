package diskCacheV111.cells;

import  dmg.cells.nucleus.* ;
import  dmg.util.* ;
import  diskCacheV111.vehicles.*;

import  java.util.*;

public class PoolInfoDb {

    private Hashtable _allPools = new Hashtable() ;
    
    private class PoolDescriptor {
    
        private String    _poolName = null ;
	private List      _poolPreferences ;
	private CellPath  _poolPath ;
        
	private PoolDescriptor( 
	          String    poolName ,
		  List      poolPreferences ,
                  CellPath  poolPath ){
		  
           _poolName         = poolName ;
           _poolPreferences  = poolPreferences ;
           _poolPath         = poolPath ;
           
        }
        private List      getPreferences(){ return _poolPreferences ; }
        private String    getName(){ return _poolName ; }
        private CellPath  getPath(){ return _poolPath ; }
    }
    public void addPoolInfo( String    poolName ,
                             CellPath  poolPath ,
                             List      poolPreferences  ){
	_allPools.put(poolName,
	              new PoolDescriptor( poolName ,
					  poolPreferences ,
					  poolPath )
		      ) ;
    }
    public synchronized void removePool( String poolName ){
	_allPools.remove(poolName);
    }
    public synchronized boolean poolExists( String poolName ){
       return _allPools.containsKey(poolName) ;
    }
    /**
      *  Basic function for the PoolManager to get a list of 
      *  pool candidates, sorted by read resp. write preference.
      */
    public synchronized 
       List  getSortedListByClass( String hsm , String className , boolean forWrite ){

       hsm = hsm.toLowerCase() ;
       
       TreeSet result = new TreeSet(PoolClassAttraction.getComparator(forWrite) ) ;

       Enumeration e = _allPools.elements() ;
       while( e.hasMoreElements() ){

           PoolDescriptor desc =  (PoolDescriptor)e.nextElement() ;

           List classes =  desc.getPreferences() ;
           Iterator   f = classes.iterator() ;
           while( f.hasNext() ){
               PoolClassAttraction classAttr = (PoolClassAttraction)f.next() ;
               if( hsm.equals( classAttr.getOrganization() )       &&
                   className.equals( classAttr.getStorageClass() ) &&
                   ( (   forWrite ? 
                            classAttr.getWritePreference() :
                            classAttr.getReadPreference()   ) > 0 )   )

                       result.add( classAttr ) ;
           }
       }
       return new ArrayList( result ) ;

    }
    public synchronized 
       List  getListByClass( String hsm , String className , boolean forWrite ){

       hsm = hsm.toLowerCase() ;
       
       ArrayList result = new ArrayList() ;

       Enumeration e = _allPools.elements() ;
       while( e.hasMoreElements() ){

           PoolDescriptor desc =  (PoolDescriptor)e.nextElement() ;

           List classes =  desc.getPreferences() ;
           Iterator   f = classes.iterator() ;
           while( f.hasNext() ){
               PoolClassAttraction classAttr = (PoolClassAttraction)f.next() ;
               if( hsm.equals( classAttr.getOrganization() )       &&
                   className.equals( classAttr.getStorageClass() ) &&
                   ( (   forWrite ? 
                            classAttr.getWritePreference() :
                            classAttr.getReadPreference()   ) > 0 )   )

                       result.add( classAttr ) ;
           }
       }
       return new ArrayList( result ) ;

    }
    public synchronized 
       List  getListByStorageInfo( StorageInfo info , boolean forWrite ){

      
       String      hsm       = info.getHsm().toLowerCase() ;
       String      className = info.getStorageClass() ;       
       ArrayList   result    = new ArrayList() ;
       Enumeration e         = _allPools.elements() ;
       
       while( e.hasMoreElements() ){

           PoolDescriptor desc = (PoolDescriptor)e.nextElement() ;
           List        classes = desc.getPreferences() ;
           Iterator          f = classes.iterator() ;
           
           while( f.hasNext() ){
               PoolClassAttraction classAttr = (PoolClassAttraction)f.next() ;
               int preference = forWrite ? classAttr.getWritePreference() :
                                           classAttr.getReadPreference()  ;
                
               //
               // the hsm must fit and the preference must be > 0
               //                           
               if( hsm.equals( classAttr.getOrganization() ) &&
                   ( preference > 0                        )    ){
                  //
                  // it it's only a template, we have to check the keys,
                  // otherwise, it it's a full specified class name it
                  // must fit.
                  //
                  if( classAttr.isTemplate() ){
                      Iterator i = classAttr.getSelection() ;
                      boolean found = true ;
                      while( i.hasNext() ){
                         Map.Entry pair  = (Map.Entry)i.next() ;
                         String    value = info.getKey(pair.getKey().toString());
                         if( ( value == null ) || 
                             ! value.equals(pair.getValue()) ){ found = false ; break ; }

                      }
                      if( found )result.add( classAttr ) ;
                      
                  }else if( className.equals( classAttr.getStorageClass() ) ){

                      result.add( classAttr ) ;
                      
                  }
               }
           }
       }
       return new ArrayList( result ) ;

    }
    public synchronized CellPath getPoolPath( String poolName )
            throws Exception {
            
        Object pool = _allPools.get( poolName ) ;
        if( pool == null )
           throw new
           Exception( "Pool "+poolName+" not in list" ) ;
        return ((PoolDescriptor)pool).getPath()  ;       
    }
    public synchronized String [] getActivePools(){
       String [] result = new String[_allPools.size()] ;
       Enumeration keys = _allPools.keys() ;
       for( int i = 0 ; keys.hasMoreElements() ; i++ )
          result[i] = keys.nextElement().toString() ;
       return result ;
    }
    public synchronized StringBuffer getClassInfo( StringBuffer sb ){
        Enumeration e = _allPools.elements() ;
        while( e.hasMoreElements() ){
           PoolDescriptor pool = (PoolDescriptor)e.nextElement() ;
           String name = pool.getName() ;
           sb.append(Formats.field(name,13,Formats.LEFT)).
              append(Formats.field("(class)",30,Formats.LEFT)).
              append(Formats.field("(read)",8,Formats.RIGHT)).
              append(Formats.field("(write)",8,Formats.RIGHT)).
              append("\n");
           Iterator it = pool.getPreferences().iterator() ;
           while( it.hasNext() ){
               PoolClassAttraction attr = (PoolClassAttraction)it.next() ;
               sb.append("   ").
                  append( Formats.field(attr.getOrganization(),10,Formats.LEFT)).
                  append( Formats.field(attr.getStorageClass(),30,Formats.LEFT)) ;
               int    p  = attr.getReadPreference() ;
               String pp = p <= 0 ? "-" : ( ""+p ) ;
               sb.append( Formats.field(pp,8,Formats.RIGHT)) ;
               p  = attr.getWritePreference() ;
               pp = p <= 0 ? "-" : ( ""+p ) ;
               sb.append( Formats.field(pp,8,Formats.RIGHT)) ;
               sb.append("\n") ;
           }
       }
       return sb ;
    }
    public StringBuffer getClassInfo( StringBuffer sb , String storageClass ){
        return getClassInfo( sb ) ;
    }
    public StringBuffer getPoolInfo( StringBuffer sb , boolean extended , boolean addClasses ){
      return sb ;
    
    }
    public StringBuffer getPoolInfo( StringBuffer sb , 
                                      String poolName ,
                                      boolean extended , boolean addClasses ){

       return sb ;
    }

}
