package dmg.protocols.ber ;

   public class BerFrame {
       private BerObject _object ;
       private int       _headerSize , _payloadSize ;
       BerFrame( BerObject obj , int headerSize , int payloadSize ){
          _object      = obj ;
          _headerSize  = headerSize ;
          _payloadSize = payloadSize ;
       }
       public BerObject getObject(){ return _object ; }
       public void setObject( BerObject obj ){ _object = obj ; }
       public BerContainer getContainer(){ 
           return (BerContainer)_object ;
       }
       public int       getHeaderSize(){ return _headerSize ; }
       public int       getPayloadSize(){ return _payloadSize ; }
       private int _klass = 0 ;
       private boolean _isPrimitive = true ;
       private int _tag   = 0 ;
       private int _type  = 0 ;
       public int     getBerType(){ return _type ; }
       public int     getBerClass(){ return _klass ; }
       public boolean isPrimitive(){ return _isPrimitive ; }
       public int     getBerTag(){ return _tag ; }
       public BerFrame( byte [] data , int off , int maxSize ){
          _type = data[off++] ;
          int meta = 0 ;
          _type = _type < 0 ? ( _type + 256 ) : _type ;

          _klass       = (   _type >> 6   ) & 0x3 ;
          _isPrimitive = ( ( _type >> 5   ) & 0x1 ) == 0 ;
          _tag         = (   _type & 0x1f ) ;

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
          _payloadSize = size ;
          _headerSize  = meta ;
       }
   }
