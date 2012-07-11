package diskCacheV111.vehicles ;

import java.util.* ;
import dmg.util.* ;

public class PoolClassAttraction implements java.io.Serializable {
   private String  _organization ;
   private String  _storageClass ;
   private boolean _isTemplate      = false ;
   private HashMap _map             = null ;
   private int     _writePreference = -1 ;
   private int     _readPreference  = -1 ;
   private String  _poolName ;
   private String  _id = null ;

   public final static PoolClassComparator comparatorForWrite =
         new PoolClassComparator(true) ;
   public final static PoolClassComparator comparatorForRead =
         new PoolClassComparator(false) ;

   private static final long serialVersionUID = 5471965365309761831L;

   public static class PoolClassComparator implements Comparator {
      private boolean _forWrite = true ;
      private PoolClassComparator( boolean forWrite ){
         _forWrite = forWrite ;
      }
      public int compare( Object a , Object b ){
        if( ! ( ( a instanceof PoolClassAttraction ) &&
                ( b instanceof PoolClassAttraction )    ) ) {
            throw new
                    ClassCastException("Can only compare : " + this.getClass());
        }
          ;

        PoolClassAttraction pca = (PoolClassAttraction)a ;
        PoolClassAttraction pcb = (PoolClassAttraction)b ;

        int intA = 0 , intB = 0 ;
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
   public  Iterator getSelection(){
     if( ( ! _isTemplate ) || ( _map == null ) ) {
         return (new ArrayList()).iterator();
     }

     return _map.entrySet().iterator() ;
   }
   private void createTemplate(){
      if( _storageClass.length() == 1 ){ _isTemplate = true ; return ; }
      _isTemplate = true ;
       StringTokenizer st = new StringTokenizer(_storageClass.substring(1),"*") ;
       _map = new HashMap() ;
       while( st.hasMoreTokens()  ){
         String selection = st.nextToken() ;
         StringTokenizer st2 = new StringTokenizer( selection , "=" ) ;
         try{
            _map.put( st2.nextToken() , st2.nextToken() ) ;
         }catch(NoSuchElementException nsee){}
       }
       if( _map.size() == 0 ) {
           _map = null;
       }
       return ;
   }
   public String getOrganization(){ return _organization ; }
   public String getStorageClass(){ return _storageClass ; }
   public int    getWritePreference(){ return _writePreference  ;}
   public int    getReadPreference(){ return _readPreference ; }
   public String getPool(){ return _poolName ; }

   public static Comparator getComparator( boolean forWrite ){
     return forWrite ? PoolClassAttraction.comparatorForWrite :
                       PoolClassAttraction.comparatorForRead    ;
   }
   public void setPreferences( int readPreference , int writePreference ){
       _writePreference = writePreference ;
       _readPreference  = readPreference ;
   }
   private void makeId(){
     _id = _poolName+":"+_storageClass+"@"+_organization ;
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
      return _id+
             "={read="+_readPreference+
             ";write="+_writePreference+"}" ;
     }else{
        StringBuffer sb = new StringBuffer() ;
        sb.append(_id).append(";t;") ;
        Iterator i = getSelection() ;
        while( i.hasNext() ){
           Map.Entry entry = (Map.Entry)i.next() ;
           sb.append(entry.getKey().toString()+"="+entry.getValue()+";");
        }
        return sb.toString() ;
     }
   }
   public String toNiceString(){
        StringBuffer sb = new StringBuffer() ;
        PoolClassAttraction attr = this ;
        sb.append( Formats.field(attr.getPool(),10,Formats.LEFT)).
           append( Formats.field(attr.getOrganization(),10,Formats.LEFT)).
           append( Formats.field(attr.getStorageClass(),30,Formats.LEFT)) ;
        int    p  = attr.getReadPreference() ;
        String pp = p <= 0 ? "-" : ( ""+p ) ;
        sb.append( Formats.field(pp,8,Formats.RIGHT)) ;
        p  = attr.getWritePreference() ;
        pp = p <= 0 ? "-" : ( ""+p ) ;
        sb.append( Formats.field(pp,8,Formats.RIGHT)) ;
        return sb.toString();
   }
   public static void main( String [] args )
   {
      PoolClassAttraction pca = null ;
      TreeSet r_read  = new TreeSet( PoolClassAttraction.comparatorForWrite ) ;
      TreeSet r_write = new TreeSet( PoolClassAttraction.comparatorForRead  ) ;

      pca = new PoolClassAttraction( "pool-a" , "OSM" , "MAIN:raw" ) ;
      pca.setPreferences( 1 , 4 ) ;
      r_read.add( pca ) ; r_write.add( pca ) ;
      pca = new PoolClassAttraction( "pool-a" , "OSM" , "MAIN:raw" ) ;
      pca.setPreferences( 2 , 3 ) ;
      r_read.add( pca ) ; r_write.add( pca ) ;
      pca = new PoolClassAttraction( "pool-b" , "OSM" , "MAIN:raw" ) ;
      pca.setPreferences( 2 , 3 ) ;
      r_read.add( pca ) ; r_write.add( pca ) ;

       Iterator i = null ;
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
