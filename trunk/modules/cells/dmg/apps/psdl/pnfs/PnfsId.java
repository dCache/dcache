package dmg.apps.psdl.pnfs ;

import java.util.* ;
import java.io.* ;

public class PnfsId implements Serializable {

  byte [] _a = new byte[12] ;
  
  public PnfsId( String idString ){
     if( idString.length() != ( 2*_a.length ) )
       throw new IllegalArgumentException( "Illegal string length" );
       
     for( int i = 0 ; i < 12 ; i++ ){
        int l = Integer.parseInt( idString.substring( 2*i , 2*(i+1) ) , 16 ) ;
        _a[i] = (byte)(( l < 128 ) ? l : (  l - 256 ) ) ;
     }
  }
  public PnfsId( byte [] idBytes ){
     if( idBytes.length != _a.length )
       throw new IllegalArgumentException( "Illegal byte array length" );
     System.arraycopy( idBytes , 0 , _a , 0 , _a.length ) ;
  }
  public int getDatabaseId(){
      return  ((((int)_a[0])&0xFF) << 8 ) | (((int)_a[1])&0xFF) ;
  }
  public String toString(){ 
     return bytesToHexString( _a ) ;
  }
  public byte [] getBytes(){
     byte [] x = new byte[_a.length] ;
     System.arraycopy( _a , 0 , x , 0 , _a.length ) ;
     return x ;
  }
  public String toShortString(){ 
       StringBuffer sb = new StringBuffer() ;
       int i = 0 ;
       for( i = 0 ; i < 2 ; i ++ )
          sb.append( byteToHexString( _a[i] ) ) ;
       for( ; ( i < _a.length) && ( _a[i] == 0 ) ; i ++ ) ;
       for( ; i < _a.length ; i ++ )
          sb.append( byteToHexString( _a[i] ) ) ;
       return sb.toString() ;
  }
  public String byteToHexString( byte b ) {
       String s = Integer.toHexString( 
            ( b < 0 ) ? ( 256 + (int)b ) : (int)b  ).toUpperCase() ;
       if( s.length() == 1 )return "0"+s ;
       else return s ;
  }
  public String bytesToHexString( byte [] b ) {
       StringBuffer sb = new StringBuffer() ;
       for( int i = 0 ; i < b.length ; i ++ )
          sb.append( byteToHexString( b[i] ) ) ;
       return sb.toString() ;

  }
  public boolean equals( Object o ){
      PnfsId id = (PnfsId)o ;
      int i ;
      for( i = 0 ;
           ( i < _a.length ) && ( _a[i] == id._a[i] ) ; i++ ) ;
      return i == _a.length ;
  }
  public static void main( String [] args ){
     if( args.length < 1 ){
         System.out.println( "USAGE : ... <pnfsId>" ) ;
         System.exit(4);
     }
     try{
        PnfsId id = new PnfsId( args[0] ) ;
        byte [] idBytes = id.getBytes() ;
        PnfsId id2 = new PnfsId( idBytes ) ;
        System.out.println( "id "+id2 ) ;
        System.out.println( "id "+id2.toShortString() ) ;
        System.out.println( "db "+id2.getDatabaseId() ) ;
        System.exit(0);
     }catch( Exception e ){
        e.printStackTrace() ;
        System.exit(4);
     }
  }

}
