package dmg.apps.psdl.vehicles ;

import  java.util.* ;
import  java.io.* ;

/**
  *  
  *
  * @author Patrick Fuhrmann
  * @version 0.1, 15 Feb 1998
  */
public class  PoolInfo extends StateInfo {  
   private long      _highWater   = 0 ;
   private long      _maxFileSize = 0 ;
   private int       _priority    = 0 ;
   private String [] _hsmKeys     = null ;
   private int       _operations  = 0 ;
   
   public PoolInfo( String name ,
                    long maxSize ,
                    long maxFileSize ,
                    int  priority , 
                    int  operations ,
                    String [] hsmKeys ){
                    
      super( name , true ) ;
      _highWater   = maxSize ;
      _maxFileSize = maxFileSize ;
      _priority    = priority ;
      _hsmKeys     = hsmKeys ;
      _operations  = operations ;
   }
   public PoolInfo( String name ){ super( name , false ) ; }
   
   public long getHighWater(){  return _highWater ; }
   public int  getPriority(){   return _priority ;  }
   public int  getOperations(){  return _operations ;  }
   public String [] getHsmKeys(){
      return _hsmKeys == null ? new String[0] : _hsmKeys ;
   }
   
   public String toString() { 
     if( isUp() ){
       StringBuffer sb = new StringBuffer() ;
       
       sb.append( "PoolInfo ").append( super.toString() ) ;
       sb.append(";size="+_highWater ) ;
       sb.append(";maxFileSize="+_maxFileSize ) ;
       sb.append(";priority="+_priority ) ;
       if( _hsmKeys != null ){
          sb.append(";") ;
          for( int i = 0 ; i < _hsmKeys.length ; i++ ){
             sb.append( _hsmKeys[i] + 
                       ( i == ( _hsmKeys.length -1 ) ? "" : "," ) ) ;
          }
       }
       sb.append( ";operation=" ) ;
       if( ( _operations & 0x1 ) != 0 )sb.append( "put," ) ;
       if( ( _operations & 0x2 ) != 0 )sb.append( "get" ) ;
       
       return sb.toString();
     }else 
       return "PoolInfo "+super.toString() ;
   }
   public void toWriter( PrintWriter pw ) { 
       
       pw.println(" PoolInfo "+super.toString() ) ;
       pw.println("  Size        : "+_highWater ) ;
       pw.println("  MaxFileSize : "+_maxFileSize ) ;
       pw.println("  Priority    : "+_priority ) ;
       pw.print("  Keys        : " ) ;
       if( _hsmKeys != null ){
          for( int i = 0 ; i < _hsmKeys.length ; i++ ){
             pw.print( _hsmKeys[i] + 
                       ( i == ( _hsmKeys.length -1 ) ? "" : "," ) ) ;
          }
          pw.println("") ;
       }else{
          pw.println("No Restriction") ;
       }
       pw.print( "  Operation   : " ) ;
       if( ( _operations & 0x1 ) != 0 )pw.print( "put," ) ;
       if( ( _operations & 0x2 ) != 0 )pw.print( "get" ) ;
       pw.println( "" ) ;

    }
}
