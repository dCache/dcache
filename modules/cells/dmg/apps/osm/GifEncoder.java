package dmg.apps.osm ;
import java.io.* ;
public class GifEncoder {

   private class GifOutputStream extends FilterOutputStream {
       private GifOutputStream( OutputStream out ){
          super(out) ;
       }
       private void writeShort( int value )throws IOException {
          out.write( value&0xff ) ;
          out.write( ( value >>> 8 ) & 0xff ) ;
       }
       private int _codeStart = 0 ;
       private void startLZW( int initialCodeSize ) throws IOException{
          _inWidth = initialCodeSize + 1 ;
          out.write( initialCodeSize ) ;
          _outPos  = 0 ;
          _restOut = 8 ;
          _out     = 0 ;
          _codeStart = 1 << initialCodeSize ;
          writeLZWUnit( _codeStart ) ;
          
       }
       private int     _outPos   = 0 ;
       private int     _inWidth  = 0 ;
       private byte [] _data     = new byte[256] ;
       private int     _restOut  = 8 ;
       private int     _outWidth = 8 ;
       private int     _out      = 0 ;
       private void writeLZWUnit( int in )throws IOException {
          int restIn = _inWidth ;
          
          while( restIn > 0 ){
              int bytesMoved = Math.min( _restOut , restIn ) ;
              _out |=  (( in >> ( _inWidth - restIn ) ) & 
                        _masks[bytesMoved] ) << ( _outWidth - _restOut ) ;
              _restOut -= bytesMoved ;
               restIn  -= bytesMoved ;
              if( _restOut == 0 ){
//                 System.out.println( " --> "+_out ) ;
                 _data[_outPos++] = (byte)(_out > 127 ? (_out-256):_out) ;
//                 System.out.println( " -*> "+_data[_outPos-1] ) ;
                 if( _outPos >= 16 ){
                    write( _outPos ) ;
                    write( _data , 0 , _outPos ) ;
                    _outPos = 0 ;
                 }
                 _restOut = _outWidth ;
                 _out     = 0 ;
              }
          
          
          }
       
       }
       public void endLZW()throws IOException {
          writeLZWUnit( _codeStart + 1 ) ;
          if( _restOut < _outWidth ){
             _data[_outPos++] = (byte)(_out > 127 ? (_out-256):_out) ;
          }
          if( _outPos > 0 ){    
             write(_outPos);     
             write(_data,0,_outPos) ;
          }
//          write(1) ;
          write(0) ;
          super.flush();
       }
   }
   public GifEncoder() throws IOException {
       ByteArrayOutputStream baos = new ByteArrayOutputStream() ;
       GifOutputStream gif = new GifOutputStream(baos) ;
       
       writeHeader(gif) ;
       writeLogicalScreenDescriptor(gif);
       writeGlobalColorTable(gif) ;
       writeImageDescriptor(gif);
       
       gif.flush() ;
       
       gif.startLZW( 4 ) ;
       for( int i = 0  ; i < 16 ; i++ )
          gif.writeLZWUnit( i % 4  ) ;
       gif.writeLZWUnit( 0x12 );
       gif.endLZW() ;
       gif.flush();
       writeTrailor(gif) ;
       gif.flush() ;

      byte [] r = baos.toByteArray() ;
      for( int i = 0 ;i < r.length ; i++ ){
         int out = r[i] ;
         String x = Integer.toHexString(out) ;
         x = x.length() == 1 ? "0"+x : x ;
         System.out.println(x.substring(x.length()-2,x.length()) ) ;
      }
      OutputStream f = new FileOutputStream(new File("otto")) ;
      f.write( r , 0 , r.length ) ;
      f.close() ;
   
   }
   private byte [] _gif = { 'G' , 'I' , 'F' } ;
   private byte [] _version = { '8' , '7' , 'a' } ;
   private void writeHeader( OutputStream out ) throws IOException {
      out.write( _gif ) ;
      out.write( _version ) ;
   }
   private int _resolution = 4 ;
   private int _isSorted   = 0 ;
   private int _colorTableSize = 3 ; // ( size = 2 pow ( _coloreTableSize + 1 ) )
   private short _width  = 4 ;
   private short _height = 4 ;
   private void writeLogicalScreenDescriptor( GifOutputStream out ) 
                throws IOException{
      out.writeShort( _width ) ;
      out.writeShort( _height ) ;
      byte flag = (byte)(
                     (1                     <<7 ) |  
                     (((_resolution-1)&0x7) <<4 ) |
                     ( (_isSorted     &0x1) <<3 ) |
                     ( _colorTableSize&0x7   ) 
                  ) ;
      out.write(flag);            
      out.write( 0 ) ; // background color index 
      out.write( 0 ) ; // pixel aspect ratio
      
   }
   private void writeGlobalColorTable( GifOutputStream out )
           throws IOException{
      int size = 1 << _colorTableSize ;
      byte [] row = new byte[3] ;
      row[0] = -1 ; row[1] = 0 ; row[2] = 0 ; 
      out.write( row ) ;
      row[0] = 0 ; row[1] = -1 ; row[2] = 0 ; 
      out.write( row ) ;
      row[0] = 0 ; row[1] = 0 ; row[2] = -1 ; 
      out.write( row ) ;
      for( int i = 3 ; i < size ; i++ ){
         row[0] = 0 ; row[1] = 0 ; row[2] = -1 ; 
         out.write( row ) ;
      }
   }
   private void writeImageDescriptor( GifOutputStream out )
           throws IOException {
           
      out.write( 0x2c) ;
      out.writeShort(0) ;
      out.writeShort(0) ;
      out.writeShort( _width ) ;
      out.writeShort( _height ) ;
      byte flag = (byte) 0 ;
      out.write(flag);
            
   }
   private void writeTrailor( GifOutputStream out )
           throws IOException {
           
      out.write( 0x3b ) ;
            
   }
   private class ColorString {
      private int    _pos = 0 ;
      private int [] _ar  = new int[4] ;
      private int    _hashCode = 0 ;
      private ColorString( int initColor ){
         _ar[_pos++] = initColor ;
         _hashCode   = initColor ;
      }
      private void add( int n ){
         if( _pos >= _ar.length )_extend() ;
         _ar[_pos++]  = n ;
         _hashCode   |= n ;
      }
      private void _extend(){
         int [] x = new int[_ar.length*2] ;
         System.arraycopy( _ar , 0 , x , 0 , _ar.length ) ;
         _ar = x ;
      }
      public int hashCode(){
         return _hashCode ;
      }
      public boolean equals( Object x ){
         if( x == null )return false ;
         if( ! ( x instanceof ColorString ) )return false ;
         ColorString y = (ColorString)x ;
         if( _pos != y._pos )return false ;
         int i = 0 ; 
         for( i = 0 ; 
              ( i < _pos ) && ( _ar[i] == y._ar[i] )  ; i++ ) ;
         return i == _pos ;
      }
   }
   private static int [] _masks ={ 0x0 , 0x1 , 0x3 , 0x7 , 0xf , 
                                   0x1f , 0x3f , 0x7f  } ; 
   /*
   private byte [] mergeToBytes( byte [] inArray , int inWidth ){
       ByteArrayOutputStream baos = new ByteArrayOutputStream() ;
       DataOutputStream outStream = new DataOutputStream(baos) ;
       int mask = 0 ;
       int in   = 0 ;
       int out  = 0 ;
       int outWidth = 8 ;  // defined
       int restOut  = outWidth ;
       int restIn   = 0 ;
       int pos      = 0 ;
       int outByte  = 0 ;
       int bytesMoved = 0 ;
       while(  true ){
           if( restIn == 0 ){
              if( pos == inArray.length )break ;
              in = inArray[pos++] ;
              restIn = inWidth ;
           }
           bytesMoved = Math.min( restOut , restIn ) ;
           out |= (( in >> ( inWidth - restIn ) ) & 
                     _masks[bytesMoved] ) << ( outWidth - restOut ) ;
           restOut -= bytesMoved ;
           restIn  -= bytesMoved ;
           outByte =   out>127?(out-256):out ;
           if( restOut == 0 ){
              try{ outStream.write( outByte ) ; }catch(Exception ee){}
              restOut = outWidth ;
              out     = 0 ;
           }
       }
       try{ outStream.flush() ; }catch(Exception ee){}
       return baos.toByteArray() ;
          
   
   }
   */
   public static void main( String [] args ) throws IOException {
      GifEncoder x = new GifEncoder() ;
   }
   

}
