package diskCacheV111.vehicles ;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.StringTokenizer;
import java.util.TreeSet;

import dmg.util.Formats;

public class PoolClassAttraction implements Serializable {
   private final String  _organization ;
   private final String  _storageClass ;
   private boolean _isTemplate;
   private Map<String, String> _map;
   private int     _writePreference = -1 ;
   private int     _readPreference  = -1 ;
   private final String  _poolName ;
   private String  _id;

   public static final PoolClassComparator comparatorForWrite =
         new PoolClassComparator(true) ;
   public static final PoolClassComparator comparatorForRead =
         new PoolClassComparator(false) ;

   private static final long serialVersionUID = 5471965365309761831L;

   public static class PoolClassComparator implements Comparator<PoolClassAttraction> {
      private boolean _forWrite = true ;
      private PoolClassComparator( boolean forWrite ){
         _forWrite = forWrite ;
      }
      @Override
      public int compare( PoolClassAttraction pca , PoolClassAttraction pcb ){
        int intA, intB;
        if( _forWrite ){
           intA = pca.getWritePreference() ;
           intB = pcb.getWritePreference() ;
        }else{
           intA = pca.getReadPreference() ;
           intB = pcb.getReadPreference() ;
        }
        return intA == intB ? pca._id.compareTo( pcb._id ) : intA < intB ? 1 : -1 ;
      }
   }
   public PoolClassAttraction(
             String poolName ,
             String organization ,
             String storageClass  ){
       _poolName        = poolName ;
       _organization    = organization.toLowerCase()  ;
       _storageClass    = storageClass ;

       if( _storageClass.startsWith("*") ) {
           createTemplate();
       }

       makeId() ;
   }
   public boolean isTemplate(){ return _isTemplate ; }
   public  Iterator<Map.Entry<String,String>> getSelection(){
     if( ( ! _isTemplate ) || ( _map == null ) ) {
         return (new ArrayList<Map.Entry<String,String>>()).iterator();
     }

     return _map.entrySet().iterator() ;
   }
   private void createTemplate(){
      if( _storageClass.length() == 1 ){ _isTemplate = true ; return ; }
      _isTemplate = true ;
       StringTokenizer st = new StringTokenizer(_storageClass.substring(1),"*") ;
       _map = new HashMap<>() ;
       while( st.hasMoreTokens()  ){
         String selection = st.nextToken() ;
         StringTokenizer st2 = new StringTokenizer( selection , "=" ) ;
         try{
            _map.put( st2.nextToken() , st2.nextToken() ) ;
         }catch(NoSuchElementException nsee){}
       }
       if(_map.isEmpty()) {
           _map = null;
       }
   }
   public String getOrganization(){ return _organization ; }
   public String getStorageClass(){ return _storageClass ; }
   public int    getWritePreference(){ return _writePreference  ;}
   public int    getReadPreference(){ return _readPreference ; }
   public String getPool(){ return _poolName ; }

   public static Comparator<PoolClassAttraction> getComparator(boolean forWrite){
     return forWrite ? PoolClassAttraction.comparatorForWrite :
                       PoolClassAttraction.comparatorForRead    ;
   }
   public void setPreferences( int readPreference , int writePreference ){
       _writePreference = writePreference ;
       _readPreference  = readPreference ;
   }
   private void makeId(){
     _id = _poolName + ':' + _storageClass + '@' + _organization ;
   }
   public void   setWritePreference( int writePreference ){
      _writePreference = writePreference ;
   }
   public void   setReadPreference( int readPreference ){
      _readPreference = readPreference ;
   }
   public boolean equals( Object obj ){
      if( ! ( obj instanceof PoolClassAttraction ) ) {
          return false;
      }
      PoolClassAttraction o = (PoolClassAttraction)obj ;
      return ( o._id ).equals( _id ) ;
   }
   public int hashCode(){
      return _id.hashCode() ;
   }
   public String toString(){
     if( ! _isTemplate ){
      return _id +
             "={read=" + _readPreference +
             ";write=" + _writePreference + '}';
     }else{
        StringBuilder sb = new StringBuilder() ;
        sb.append(_id).append(";t;") ;
        Iterator<Map.Entry<String,String>> i = getSelection() ;
        while( i.hasNext() ){
           Map.Entry<String, String> entry = i.next();
           sb.append(entry.getKey()).append('=')
                   .append(entry.getValue()).append(';');
        }
        return sb.toString() ;
     }
   }
   public String toNiceString(){
        StringBuilder sb = new StringBuilder() ;
        PoolClassAttraction attr = this ;
        sb.append( Formats.field(attr.getPool(), 10, Formats.LEFT)).
           append( Formats.field(attr.getOrganization(),10,Formats.LEFT)).
           append( Formats.field(attr.getStorageClass(),30,Formats.LEFT)) ;
        int    p  = attr.getReadPreference() ;
        String pp = p <= 0 ? "-" : (String.valueOf(p)) ;
        sb.append( Formats.field(pp,8,Formats.RIGHT)) ;
        p  = attr.getWritePreference() ;
        pp = p <= 0 ? "-" : (String.valueOf(p)) ;
        sb.append( Formats.field(pp,8,Formats.RIGHT)) ;
        return sb.toString();
   }
   public static void main( String [] args )
   {
      PoolClassAttraction pca;
      TreeSet<PoolClassAttraction> r_read  = new TreeSet<>( PoolClassAttraction.comparatorForWrite ) ;
      TreeSet<PoolClassAttraction> r_write = new TreeSet<>( PoolClassAttraction.comparatorForRead  ) ;

      pca = new PoolClassAttraction( "pool-a" , "OSM" , "MAIN:raw" ) ;
      pca.setPreferences( 1 , 4 ) ;
      r_read.add( pca ) ; r_write.add( pca ) ;
      pca = new PoolClassAttraction( "pool-a" , "OSM" , "MAIN:raw" ) ;
      pca.setPreferences( 2 , 3 ) ;
      r_read.add( pca ) ; r_write.add( pca ) ;
      pca = new PoolClassAttraction( "pool-b" , "OSM" , "MAIN:raw" ) ;
      pca.setPreferences( 2 , 3 ) ;
      r_read.add( pca ) ; r_write.add( pca ) ;

       Iterator<PoolClassAttraction> i;
       i = r_read.iterator() ;
       while( i.hasNext() ) {
           System.out.println("Read : " + i.next());
       }
       i = r_write.iterator() ;
       while( i.hasNext() ) {
           System.out.println("Write : " + i.next());
       }

   }
}
