 // $Id: HttpFlushManagerHelper.java,v 1.2 2006-05-06 19:46:41 patrick Exp $
package diskCacheV111.hsmControl.flush ;

import dmg.util.* ;
import dmg.cells.nucleus.* ;
import java.io.* ;
import java.util.* ;
import diskCacheV111.vehicles.* ;
import diskCacheV111.vehicles.hsmControl.*;
import diskCacheV111.util.* ;
import java.text.*;

class HttpFlushManagerHelper {
   /**
     *  Helpers and their comparators
     */
   static class PoolEntry {
   
      String  _poolName     = null ;
      long    _total        = 0 ;
      long    _precious     = 0 ;
      int     _flushing     = 0 ;
      boolean _isReadOnly   = false ;
      String  _storageClass = null ;
   
   }
   static abstract private class EntryComparator implements Comparator {
       boolean _topHigh    = false ;
       int     _sortColumn = 0 ;
       void setColumn( int column ){ 
       
          if( _sortColumn == column ){
              _topHigh = ! _topHigh ;
          }else{
              _sortColumn = column ;
              _topHigh    = false ;
          } 
       }
       abstract public int compare( Object a , Object b ) ;
       int compareBoolean( boolean a , boolean b ){
          return a ^ b ? ( a ? 1 : -1 ) : 0 ;
       }
       int compareInt( int a , int b ){
          return a == b ? 0 : a > b ? 1 : -1 ;
       }
       int compareLong( long a , long b ){
          return a == b ? 0 : a > b ? 1 : -1 ;
       }
       int compareDouble( double a , double b ){
          return a == b ? 0 : a > b ? 1 : -1 ;
       }
   }
   static class PoolEntryComparator extends EntryComparator {
   
       public int compare( Object a , Object b ){
          PoolEntry [] info1 = { (PoolEntry)a , (PoolEntry)b  } ;
          PoolEntry [] info2 = { (PoolEntry)b , (PoolEntry)a  } ;
	  PoolEntry [] info  = _topHigh ? info1 : info2 ;
	  int t = 0 ;
	  switch(_sortColumn){ 
	     case 0 :
	        return info[0]._poolName.compareTo( info[1]._poolName ) ;
	     case 1 :
	        t = compareBoolean( info[0]._isReadOnly , info[1]._isReadOnly ) ;
	        return  t == 0 ? info[0]._poolName.compareTo( info[1]._poolName ) : t ;
	     case 2 :
	        t = compareInt( info[0]._flushing , info[1]._flushing ) ;
	        return  t == 0 ? info[0]._poolName.compareTo( info[1]._poolName ) : t ;
	     case 3 :
	        t = compareLong( info[0]._total , info[1]._total ) ;
 	        return  t == 0 ? info[0]._poolName.compareTo( info[1]._poolName ) : t ;
	     case 4 :
	        t = compareLong( info[0]._precious , info[1]._precious)  ;
 	        return  t == 0 ? info[0]._poolName.compareTo( info[1]._poolName ) : t ;
	     case 5 :
	        return compareDouble( 
                            (double)info[0]._precious / (double)info[0]._total  ,
                            (double)info[1]._precious / (double)info[1]._total     ) ;
	     default : return 0 ;
	  }
	  
       }
   }
   static class FlushEntryComparator extends EntryComparator {
   
       public int compare( Object a , Object b ){
          FlushEntry [] info1 = { (FlushEntry)a , (FlushEntry)b  } ;
          FlushEntry [] info2 = { (FlushEntry)b , (FlushEntry)a  } ;
	  FlushEntry [] info  = _topHigh ? info1 : info2 ;
	  int t = 0 ;
	  switch(_sortColumn){ 
	     case 0 :
	        t = info[0]._poolName.compareTo( info[1]._poolName ) ;
                return t == 0 ? info[0]._storageClass.compareTo( info[1]._storageClass ) : t ;
	     case 1 :
	        t = info[0]._storageClass.compareTo( info[1]._storageClass ) ;
                return t == 0 ? info[0]._poolName.compareTo( info[1]._poolName ) : t ;
	     case 2 :
	        t = compareBoolean( info[0]._isFlushing , info[1]._isFlushing ) ;
                t = t == 0 ? info[0]._poolName.compareTo( info[1]._poolName ) : t ;
	        return  t == 0 ? info[0]._storageClass.compareTo( info[1]._storageClass ) : t ;
	     case 3 :
	        t = compareLong( info[0]._total , info[1]._total ) ;
 	        t = t == 0 ? info[0]._poolName.compareTo( info[1]._poolName ) : t ;
	        return  t == 0 ? info[0]._storageClass.compareTo( info[1]._storageClass ) : t ;
	     case 4 :
	        t = compareLong( info[0]._precious , info[1]._precious)  ;
 	        t = t == 0 ? info[0]._poolName.compareTo( info[1]._poolName ) : t ;
	        return  t == 0 ? info[0]._storageClass.compareTo( info[1]._storageClass ) : t ;
	     case 5 :
	        t = compareInt( info[0]._active , info[1]._active)  ;
 	        t = t == 0 ? info[0]._poolName.compareTo( info[1]._poolName ) : t ;
	        return  t == 0 ? info[0]._storageClass.compareTo( info[1]._storageClass ) : t ;
	     case 6 :
	        t = compareInt( info[0]._pending , info[1]._pending)  ;
 	        t = t == 0 ? info[0]._poolName.compareTo( info[1]._poolName ) : t ;
	        return  t == 0 ? info[0]._storageClass.compareTo( info[1]._storageClass ) : t ;
	     case 7 :
	        t = compareInt( info[0]._failed , info[1]._failed)  ;
 	        t = t == 0 ? info[0]._poolName.compareTo( info[1]._poolName ) : t ;
	        return  t == 0 ? info[0]._storageClass.compareTo( info[1]._storageClass ) : t ;
 	     case 8 :
	        return compareDouble( 
                            (double)info[0]._precious / (double)info[0]._total  ,
                            (double)info[1]._precious / (double)info[1]._total     ) ;
	     default : return 0 ;
	  }
	  
       }
   }
   static class FlushEntry {
   
      String  _poolName     = null ;
      boolean _isFlushing   = false ;
      String  _storageClass = null ;
      long    _total        = 0 ;
      long    _precious     = 0 ;
      int     _active       = 0 ;
      int     _pending      = 0 ;
      int     _failed       = 0 ;
   
   }
}
