package dmg.security.cipher ;


public class StreamFromBlockCipher implements StreamCipher {
  
  BlockCipher _generator ;
  byte  []    _vector  ;
  int         _pointer;
  int         _blockLength;
  int         _mode;
  
  private static final int MODE_CFB  = 1 ;
  private static final int MODE_CBC  = 2 ;
  private static final int MODE_CBC_EXP  = 3 ;
  
  public StreamFromBlockCipher( BlockCipher generator , byte [] vector ){
     _StreamFromBlockCipher(   generator ,  vector ,   null  ) ;
  }
  public StreamFromBlockCipher( BlockCipher generator , byte [] vector , String mode ){
     _StreamFromBlockCipher(   generator ,  vector ,   mode ) ;
  }
  public void _StreamFromBlockCipher( BlockCipher generator , byte [] vector , String mode ){
  
     _generator   = generator ;
     _pointer     = 0 ;
     _blockLength = generator.getBlockLength() ;
     if( mode == null ){
     }else if( mode.equals( "cfb" ) ){
        _mode = MODE_CFB ;
     }else if( mode.equals( "cbc" ) ){
        _mode = MODE_CBC ;
     }else if( mode.equals( "cbc-exp" ) ){
        _mode = MODE_CBC_EXP ;
     }else {
         throw new IllegalArgumentException("Mode not supported : " + mode);
     }
     
     int block    = _blockLength / 8 ;
     
     if( block > vector.length ) {
         throw new
                 IllegalArgumentException("Cipher Block Length doesn't match vector length");
     }
     _vector = new byte[block] ;
     System.arraycopy( vector , 0 , _vector , 0 , block ) ;
  }
  @Override
  public int     getBlockLength(){ return _generator.getBlockLength() ; }
  @Override
  public byte [] getKeyBytes()   { return _generator.getKeyBytes() ;    }
  
  @Override
  public void    encrypt( byte [] inBlock  , int inOff ,
                           byte [] outBlock , int outOff , int len ){
     switch( _mode ){
     
        case MODE_CFB : encryptCFB( inBlock, inOff, outBlock, outOff , len ) ;
        break ;
        case MODE_CBC : encryptCBC( inBlock, inOff, outBlock, outOff , len ) ;
        break ;
        case MODE_CBC_EXP : encryptCBC_EXP( inBlock, inOff, outBlock, outOff , len ) ;
        break ;
        default : throw new IllegalArgumentException("No Mode selected" ) ;
        
     }
  }
  @Override
  public void    decrypt( byte [] inBlock  , int inOff ,
                           byte [] outBlock , int outOff , int len ){
     switch( _mode ){
     
        case MODE_CFB : decryptCFB( inBlock, inOff, outBlock, outOff , len ) ;
        break ;
        case MODE_CBC : decryptCBC( inBlock, inOff, outBlock, outOff , len ) ;
        break ;
        case MODE_CBC_EXP : decryptCBC_EXP( inBlock, inOff, outBlock, outOff , len ) ;
        break ;
        default : throw new IllegalArgumentException("No Mode selected" ) ;
        
                                                                                   }
  }
  public void encryptECB( byte [] in , int inOff , byte [] out , int outOff ){   
     _generator.encrypt( in , inOff , out , outOff ) ;
  }
  public void decryptECB( byte [] in , int inOff , byte [] out , int outOff ){   
     _generator.decrypt( in , inOff , out , outOff ) ;
  }
  public void encryptCFB( byte [] in  , int inOff  ,
                          byte [] out , int outOff , int len  ){
                             
     for( int i = 0 ; i < len ; i++ ){
        if( _pointer == 0 ) {
            _generator.encrypt(_vector, 0, _vector, 0);
        }
           _vector[_pointer] = 
           out[i+outOff]     = (byte)(_vector[_pointer] ^ in[i+inOff]) ;
           _pointer = ( _pointer + 1 ) & 0x7 ;       
     }
  }
  public void decryptCFB( byte [] in  , int inOff  ,
                          byte [] out , int outOff , int len  ){
     //
     // !!! cipher feedback mode : both 
     //     directions are running in encrypt mode 
     //                       
     for( int i = 0 ; i < len ; i++ ){
        if( _pointer == 0 ) {
            _generator.encrypt(_vector, 0, _vector, 0);
        }
        out[i+outOff] = (byte) ( _vector[_pointer] ^ 
                               ( _vector[_pointer] = in[i+inOff] ) );
        _pointer = ( _pointer + 1 ) & 0x7 ;       
     }
     
  }
  static public String byteToHexString( byte b ) {
       String s = Integer.toHexString( ( b < 0 ) ? ( 256 + (int)b ) : (int)b  ) ;
       if( s.length() == 1 ) {
           return "0" + s;
       } else {
           return s;
       }
  }
  static public String byteToHexString( byte [] b ) {
       return  byteToHexString(b, 0, b.length);
    
  }
  static public String byteToHexString( byte [] b , int off , int len ) {
      
	  StringBuilder sb = new StringBuilder(b.length +1);
	  
       for( int i = off ; i < (off + len) ; i ++ ) {
          sb.append(byteToHexString( b[i] ) ).append(" " ) ;
       }
       return sb.toString() ;    
  }
  public void encryptCBC( byte [] in  , int inOff  ,
                          byte [] out , int outOff , int len  ){
     int block = _blockLength / 8 ;
     
     if( ( len % block ) != 0 ) {
         throw new IllegalArgumentException("len not block aligned");
     }
                      
     for( int i = 0 ; i < len ; i+= block ){
     
//        for( int j = 0 ; j < block ; j++ )           
        _vector[0] = (byte)(in[inOff+i+3] ^ _vector[0]) ; 
        _vector[1] = (byte)(in[inOff+i+2] ^ _vector[1]) ; 
        _vector[2] = (byte)(in[inOff+i+1] ^ _vector[2]) ; 
        _vector[3] = (byte)(in[inOff+i+0] ^ _vector[3]) ; 
        _vector[4] = (byte)(in[inOff+i+7] ^ _vector[4]) ; 
        _vector[5] = (byte)(in[inOff+i+6] ^ _vector[5]) ; 
        _vector[6] = (byte)(in[inOff+i+5] ^ _vector[6]) ; 
        _vector[7] = (byte)(in[inOff+i+4] ^ _vector[7]) ; 
         
        _generator.encrypt( _vector , 0 , _vector , 0  ) ;

        System.arraycopy( _vector , 0 , out , outOff+i , block ) ;
        out[outOff+i+0] = _vector[3] ;
        out[outOff+i+1] = _vector[2] ;
        out[outOff+i+2] = _vector[1] ;
        out[outOff+i+3] = _vector[0] ;
        out[outOff+i+4] = _vector[7] ;
        out[outOff+i+5] = _vector[6] ;
        out[outOff+i+6] = _vector[5] ;
        out[outOff+i+7] = _vector[4] ;
    }
  }
  public void encryptCBC_EXP( byte [] in  , int inOff  ,
                              byte [] out , int outOff , int len  ){
     int block = _blockLength / 8 ;
     
     if( ( len % block ) != 0 ) {
         throw new IllegalArgumentException("len not block aligned");
     }
                      
     for( int i = 0 ; i < len ; i+= block ){
     
        for( int j = 0 ; j < block ; j++ ) {
            _vector[j] = (byte) (in[inOff + i + j] ^ _vector[j]);
        }
         
        _generator.encrypt( _vector , 0 , _vector , 0  ) ;

        System.arraycopy( _vector , 0 , out , outOff+i , block ) ;
        for( int j = 0 ; j < block ; j++ ) {
            out[outOff + i + j] = _vector[j];
        }
    }
  }
  private void swap( byte [] b , int off ){
     byte tmp = b[off+0] ;
     b[off+0] = b[off+3] ;
     b[off+3] = tmp ;
     tmp = b[off+1] ;
     b[off+1] = b[off+2] ;
     b[off+2] = tmp ;

  }
  public void decryptCBC( byte [] in  , int inOff  ,
                          byte [] out , int outOff , int len  ){
 
     int block = _blockLength / 8 ;
     byte [] tmp   = new byte[block] ;
     byte [] tmp2  = new byte[block] ;
     
     if( ( len % block ) != 0 ) {
         throw new IllegalArgumentException("len not block aligned");
     }
                      
     for( int i = 0 ; i < len ; i+= block ){
     
        tmp[0] = in[inOff+i+3] ; 
        tmp[1] = in[inOff+i+2] ; 
        tmp[2] = in[inOff+i+1] ; 
        tmp[3] = in[inOff+i+0] ; 
        tmp[4] = in[inOff+i+7] ; 
        tmp[5] = in[inOff+i+6] ; 
        tmp[6] = in[inOff+i+5] ; 
        tmp[7] = in[inOff+i+4] ; 
        _generator.decrypt( tmp , 0 , tmp2 , 0 ) ;
        out[outOff+i+0] = (byte)(tmp2[3] ^ _vector[0]) ; 
        out[outOff+i+1] = (byte)(tmp2[2] ^ _vector[1]) ; 
        out[outOff+i+2] = (byte)(tmp2[1] ^ _vector[2]) ; 
        out[outOff+i+3] = (byte)(tmp2[0] ^ _vector[3]) ; 
        out[outOff+i+4] = (byte)(tmp2[7] ^ _vector[4]) ;
        out[outOff+i+5] = (byte)(tmp2[6] ^ _vector[5]) ; 
        out[outOff+i+6] = (byte)(tmp2[5] ^ _vector[6]) ; 
        out[outOff+i+7] = (byte)(tmp2[4] ^ _vector[7]) ; 
         
        _vector[0] =  tmp[3] ;        
        _vector[1] =  tmp[2] ;        
        _vector[2] =  tmp[1] ;        
        _vector[3] =  tmp[0] ;        
        _vector[4] =  tmp[7] ;        
        _vector[5] =  tmp[6] ;        
        _vector[6] =  tmp[5] ;        
        _vector[7] =  tmp[4] ;        

     }
     
  }
  public void decryptCBC_EXP( byte [] in  , int inOff  ,
                          byte [] out , int outOff , int len  ){
 
     int block = _blockLength / 8 ;
     byte [] tmp   = new byte[block] ;
     byte [] tmp2  = new byte[block] ;
     
     if( ( len % block ) != 0 ) {
         throw new IllegalArgumentException("len not block aligned");
     }
                      
     for( int i = 0 ; i < len ; i+= block ){
     
        tmp[0] = in[inOff+i+0] ; 
        tmp[1] = in[inOff+i+1] ; 
        tmp[2] = in[inOff+i+2] ; 
        tmp[3] = in[inOff+i+3] ; 
        tmp[4] = in[inOff+i+4] ; 
        tmp[5] = in[inOff+i+5] ; 
        tmp[6] = in[inOff+i+6] ; 
        tmp[7] = in[inOff+i+7] ; 
        _generator.decrypt( tmp , 0 , tmp2 , 0 ) ;
        out[outOff+i+0] = (byte)(tmp2[0] ^ _vector[0]) ; 
        out[outOff+i+1] = (byte)(tmp2[1] ^ _vector[1]) ; 
        out[outOff+i+2] = (byte)(tmp2[2] ^ _vector[2]) ; 
        out[outOff+i+3] = (byte)(tmp2[3] ^ _vector[3]) ; 
        out[outOff+i+4] = (byte)(tmp2[4] ^ _vector[4]) ;
        out[outOff+i+5] = (byte)(tmp2[5] ^ _vector[5]) ; 
        out[outOff+i+6] = (byte)(tmp2[6] ^ _vector[6]) ; 
        out[outOff+i+7] = (byte)(tmp2[7] ^ _vector[7]) ; 
         
        _vector[0] =  tmp[0] ;        
        _vector[1] =  tmp[1] ;        
        _vector[2] =  tmp[2] ;        
        _vector[3] =  tmp[3] ;        
        _vector[4] =  tmp[4] ;        
        _vector[5] =  tmp[5] ;        
        _vector[6] =  tmp[6] ;        
        _vector[7] =  tmp[7] ;        

     }
     
  }
  public static void main( String [] args ){
     byte a , b , c ;
     a = 10 ;
     b = -10 ;
     c = (byte)( a ^ b ) ;
     System.out.println( " c : " + c ) ;
  }


}
