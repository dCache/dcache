package dmg.protocols.ber ;

import java.io.* ;
import java.util.* ;
import dmg.protocols.kerberos.* ;


public class BerObject implements java.io.Serializable {

   public static final int UNIVERSAL   = 0 ;
   public static final int APPLICATION = 1 ;
   public static final int CONTEXT     = 2 ;
   public static final int PRIVATE     = 3 ;
   
   private static String [] __classes = { "U" , "A" , "C" , "P" } ;
   
   
   
   private int     _klass;
   private boolean _primitive = true ;
   private int     _tag;
   private int     _type;
   private byte [] _data;
   
   public BerObject( int berClass , boolean isPrimitive , int tag ,
                     byte [] data , int offset , int size  ){
      this( berClass , isPrimitive , tag ) ;
      _data = new byte[size] ;
      System.arraycopy( data , offset , _data , 0 , size ) ;
                     
   }
   public BerObject( int berClass , boolean isPrimitive , int tag ){
      _klass     = berClass ;
      _primitive = isPrimitive ;
      _tag       = tag ;
      _type      = ( _klass << 6 ) |_tag | ( _primitive ? 0 : 0x20 ) ;
   }
   public byte [] getEncodedData( byte [] data ){
      byte [] type = getEncodedType() ;
      byte [] len  = getEncodedLength(data.length) ;
      byte [] result = new byte[data.length+type.length+len.length] ;
      int p = 0 ;
      System.arraycopy( type , 0 , result , p , type.length ) ;
      p += type.length ;
      System.arraycopy( len  , 0 , result , p , len.length ) ;
      p += len.length ;
      System.arraycopy( data , 0 , result , p , data.length ) ;
      return result ;
   }
   public static void displayHex( byte [] data ){
      Base64.displayHex( data ) ;
   }
   public static byte [] getEncodedLength( int len ){
      byte [] x = null ;
      if( len < 128  ){
         x = new byte[1] ;
         x[0] = (byte)len ;
      }else{
         x = new byte[3] ;
         int tmp = 128 + 2 ;
         tmp = tmp > 128 ? ( tmp - 256 ) : tmp ;
         x[0] = (byte)tmp ;
         tmp = len & 0xff ;
         tmp = tmp > 128 ? ( tmp - 256 ) : tmp ;
         x[2] = (byte)tmp ;
         tmp = ( len >>> 8 ) & 0xff ;
         tmp = tmp > 128 ? ( tmp - 256 ) : tmp ;
         x[1] = (byte)tmp ;
      }
      return x ;
   }
   public byte [] getEncodedType(){
     byte [] x = new byte[1] ;
     x[0] = (byte)(_type > 128 ? ( _type - 256 ) : _type) ;
     return x ;
   }
   public boolean isPrimitive(){ return _primitive ; }
   public int     getTag(){ return _tag ; }
   public int     getBerClass(){ return _klass ; }
   public int     getType(){ return _type ; }
   public byte [] getData(){ return _data ; }
   
   public byte [] getEncodedData(){
      if( _data == null ) {
          throw new
                  IllegalArgumentException("BerObject.getEncodedData now overwritten");
      }
      return getEncodedData( _data ) ;
   }
   public static BerFrame decode( byte [] data , int off , int maxSize ){
      int type = data[off++] ;
      int meta = 0 ;
      type = type < 0 ? ( type + 256 ) : type ;
      
      int     klass       = (   type >> 6   ) & 0x3 ;
      boolean isPrimitive = ( ( type >> 5   ) & 0x1 ) == 0 ;
      int     tag         = (   type & 0x1f ) ;
      
      meta ++ ;
      
      int highSize = data[off++] ;
      meta++ ;
      highSize = highSize < 0 ? ( highSize + 256 ) : highSize ;
      int size = 0 ;
      if( ( highSize & 0x80 ) != 0 ){
         int octects = highSize & 0x7f ;
         for( int i = 0 ; i < octects ; i++ ){
            int d = data[off++] ;
            meta++ ;
            d = d < 0 ? ( d + 256 ) : d ;
            size <<= 8 ;
            size += d ;
         }
      }else{
         size = highSize & 0x7f ;
      }
      
      BerObject ber = null ;
      
      if( isPrimitive ){
         if( tag == 27 ){
            ber = new BerGeneralString( data , off , size ) ;
         }else if( tag == 1 ){
            ber = new BerTokenId(1) ;
         }else if( tag == 2 ){
            ber = new BerInteger( data , off , size ) ;
         }else if( tag == 3 ){
            ber = new BerBitString( data , off , size ) ;
         }else if( tag == 4 ){
            ber = new BerOctectString( data , off , size ) ;
         }else if( tag == 6 ){
            ber = new BerObjectIdentifier( data , off , size ) ;
         }else if( tag == 24 ){
            ber = new KerberosTime( data , off , size ) ;
         }else{
            ber = new BerObject( klass , isPrimitive , tag ,
                                 data , off , size ) ;
         }
      }else{
         ber = new BerContainer( klass , tag , data , off , size ) ;
         
      }
      return new BerFrame( ber , meta , size ) ;
   
   }
   public String getTypeCode(){
      String s = Integer.toHexString(_type) ;
      return s.length() == 1 ? "0"+s : s ;
   }
   public String getTypeString(){ 
      return "T"+getTypeCode()+"-("+__classes[_klass]+(_primitive?"P":"C")+_tag+")" ;
   }
   public static void scanBER( byte [] data ){
      scanBER( new PrintWriter( new OutputStreamWriter( System.out ) ) ,
               0 ,
               data , 0 ) ;
   }
   public void printNice(){ printNice(0) ;}
   public void printNice( int level ){
      for( int i = 0 ; i < (level*3) ; i++ ) {
          System.out.print(" ");
      }
      System.out.print(getTypeString()+" ");
      System.out.println( toString() ) ;
   }
   public String toString(){
      if( _data == null ) {
          return " (noInfo) ";
      }
      StringBuilder sb = new StringBuilder() ;
      for( int i = 0 ; i < _data.length ; i++ ){
         sb.append( Base64.byteToHex(_data[i]) ).append(" ") ;
      }
      return sb.toString() ;
   }
   public static int  scanBER( PrintWriter pw , int level ,
                               byte [] data , int off ){
      int type = data[off++] ;
      int meta = 0 ;
      type = type < 0 ? ( type + 256 ) : type ;
      
      int     klass       = (   type >> 6   ) & 0x3 ;
      boolean isPrimitive = ( ( type >> 5   ) & 0x1 ) == 0 ;
      int     tag         = (   type & 0x1f ) ;
      
      meta ++ ;
      
      int highSize = data[off++] ;
      meta++ ;
      highSize = highSize < 0 ? ( highSize + 256 ) : highSize ;
      int size = 0 ;
      if( ( highSize & 0x80 ) != 0 ){
         int octects = highSize & 0x7f ;
         for( int i = 0 ; i < octects ; i++ ){
            int d = data[off++] ;
            meta++ ;
            d = d < 0 ? ( d + 256 ) : d ;
            size <<= 8 ;
            size += d ;
         }
      }else{
         size = highSize & 0x7f ;
      }
      
      
      String  mode = "Class("+Integer.toHexString(type)+")="+__classes[klass]+";"+
                    (isPrimitive?"P":"C")+";"+
                     "T="+tag ;
                     
      for( int i = 0 ; i < (level*3) ; i++ ) {
          System.out.print(" ");
      }
      System.out.print( ""+off+":"+mode ) ;
//      System.out.println(";meta="+meta+";size="+size);
      System.out.print(";size="+size);
      
      if( isPrimitive ){
         System.out.print(" : ");
         
         if( tag == 27 ){
            StringBuilder sb = new StringBuilder();
            for( int i = 0 ; i < size ; i++ ) {
                sb.append((char) data[off + i]);
            }
            System.out.print( sb.toString() ) ;
         }else if( tag == 2 ){
            long l = 0 ;
            for( int i = 0 ; i < size ; i++ ){
               l <<= 8 ;
               int b = data[off+i] ;
               l += ( b < 0 ? ( b + 256 ) : b ) ;
            }
            System.out.print( " Integer="+l ) ;
         }else if( tag == 2 ){
            System.out.print( " BitString= " );
            for( int i = 0 ; i < size ; i++ ){
               System.out.print( Base64.byteToHex( data[off+i] ) +" ");
            }
         }else if( tag == 6 ){
            int a = data[off] ;
            a = ( a < 0 ? ( a + 256 ) : a ) ;
            int x = a / 40 ;
            int y = a % 40 ;
            System.out.print( " OID="+x+"."+y+"." ) ;
            long l = 0 ;
            for( int i = 1 ; i < size ; i++ ){
               a = data[off+i] ;
               a = ( a < 0 ? ( a + 256 ) : a ) ; 
               l <<= 7 ;
               l += a & 0x7f ;
               if( ( a & 0x80 ) == 0 ){
                  System.out.print(""+l+".");  
                  l = 0 ;     
               }   
            }
         }else{
            for( int i = 0 ; i < size ; i++ ){
               System.out.print( Base64.byteToHex( data[off+i] ) +" ");
            }
         }
         System.out.println("");
      }else{
         System.out.println("");
         for( int sum = 0 ; sum < size ; ){
            int c = scanBER( pw , level+1, data , off + sum ) ;
            sum += c ;
         }      
      }
      return size + meta ;
   }
   public static void main( String [] args )
   {
      if( args.length == 0 ){
         System.err.println( "Usage : ... <b0> <b1> ..." ) ;
         System.exit(4);
      }
      byte [] data = new byte[args.length] ;
      for( int i = 0 ; i < data.length ; i++ ){
        int n = Integer.parseInt( args[i] , 16 ) ;
        data[i] = (byte)( n > 128 ? n - 256 : n ) ;
      }
      BerObject.decode( data , 0 , data.length ).getObject().printNice() ;
      System.exit(0);
   }
}
