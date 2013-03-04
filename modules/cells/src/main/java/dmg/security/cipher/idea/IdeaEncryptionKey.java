package dmg.security.cipher.idea ;

import dmg.security.cipher.EncryptionKey;

/**
  *
  *
  * @author Patrick Fuhrmann
  * @version 0.1, 15 Feb 1998
  */
public class IdeaEncryptionKey implements EncryptionKey {

    byte  [] _key        = new byte [16] ;
  String  [] _domainList;

  public IdeaEncryptionKey( String keyString  ) throws NumberFormatException  {
      this( null , keyString ) ;
  }
  public IdeaEncryptionKey( byte [] key  ) throws NumberFormatException  {
      _IdeaEncryptionKey( null , key ) ;
  }
  public IdeaEncryptionKey( String [] domainList , String keyString )
           throws NumberFormatException   {

     if( ( keyString == null ) || ( keyString.length() != 32 ) ) {
         throw new NumberFormatException("KeyLength != IdeaKeyLength(16)");
     }

     byte [] key = new byte[16] ;

     for( int i = 0 ; i < 16 ; i++ ){
         key[i] = (byte) (( _byteFromChar( keyString.charAt(2*i) ) << 4  ) |
                          ( _byteFromChar( keyString.charAt(2*i+1) ) & 0xF )  );
     }
     _IdeaEncryptionKey( domainList , key ) ;
  }
  private void _IdeaEncryptionKey( String [] domainList , byte [] key  )
           throws NumberFormatException   {

     //
     //  check for valid idea key length
     //
     if( ( key == null ) || ( key.length != 16 ) ) {
         throw new NumberFormatException("KeyLength != IdeaKeyLength(16)");
     }
     //
     // save the key
     //
     System.arraycopy( key , 0 , _key , 0 , 16 ) ;
     //
     // save the domain list
     //
     _domainList = new String[domainList.length] ;
      System.arraycopy(domainList, 0, _domainList, 0, domainList.length);
  }
  @Override
  public String [] getDomainList(){ return _domainList ; }
  public byte   [] getBytes()   {   return _key  ; }
  @Override
  public String    getKeyType() {   return "idea" ; }
  @Override
  public String    getKeyMode() {   return "shared" ; }

  public String    getString(){
     StringBuilder sb = new StringBuilder(32) ;
     for( int i = 0 ; i < 16 ; i++ ) {
         sb.append("").append(_byteToChar[+((_key[i] >>> 4) & 0xF)])
                 .append(_byteToChar[_key[i] & 0xF]);
     }
     return sb.toString() ;
  }
  public String    toString() {
    StringBuilder sb = new StringBuilder() ;
    sb.append( " ---- Idea Shared Key --------\n" ) ;
    sb.append(" Key     = ").append(getString()).append("\n");
    if( _domainList != null ){
       sb.append( " Domains = " ) ;
        for (String domain : _domainList) {
            sb.append(domain).append(" ");
        }
       sb.append("\n" ) ;
    }
    return sb.toString() ;
  }

  private final static char _byteToChar[] = {
      '0','1','2','3','4','5','6','7','8',
      '9','A','B','C','D','E','F' } ;
   private byte   _byteFromChar( char c )
           throws NumberFormatException   {
      if( (c>='0')&&(c<='9') ){
         return (byte) ( c - '0' ) ;
      }else if( (c>='a')&&(c<='f') ){
         return (byte) (( c - 'a' ) + 10 ) ;
      }else if( (c>='A')&&(c<='F') ){
         return (byte) (( c - 'A' ) + 10 ) ;
      }else {
          throw new NumberFormatException("None Hex in key string");
      }
   }
   public static void main( String [] args ) {

     if( args.length < 1 ) {
         System.exit(4);
     }

     EncryptionKey key = new IdeaEncryptionKey( args[0] ) ;

     System.out.println( " Key Type : "+key.getKeyType() ) ;
     System.out.println( " Key vale : "+key ) ;
   }

}
