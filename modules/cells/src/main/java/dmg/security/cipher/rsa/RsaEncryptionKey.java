package dmg.security.cipher.rsa ;
import  dmg.security.cipher.EncryptionKey ;
import  java.math.BigInteger ;

/**
  *  
  *
  * @author Patrick Fuhrmann
  * @version 0.1, 15 Feb 1998
  */
public class RsaEncryptionKey implements EncryptionKey {

   String []  _domainList;
   String     _mode ;
   BigInteger _n , _e ;
   
  public RsaEncryptionKey( String [] domainList ,
                           String  e , String n      ){
         
     this( domainList , "public" ,
           new BigInteger( e , 10 ) ,
           new BigInteger( n , 10 )   ) ;                                       
                   
  }
  public RsaEncryptionKey( String [] domainList , String mode , 
                           String  e , String n      ){
         
     this( domainList , mode ,
           new BigInteger( e , 16 ) ,
           new BigInteger( n , 16 )   ) ;                                       
                   
  }
  public BigInteger getModulus(){  return _n ; }
  public BigInteger getExponent(){ return _e ; }
  
  public RsaEncryptionKey( String [] domainList , String mode , 
                           BigInteger e , BigInteger n ){
         
     _e          = e ;
     _n          = n ;    
     _mode       = mode ;               
     _domainList = new String[domainList.length] ;
     for( int i = 0 ; i < domainList.length ; i++ ) {
         _domainList[i] = domainList[i];
     }
                            
                            
  }
  @Override
  public String [] getDomainList(){ return _domainList ; }
  @Override
  public String    getKeyType() {   return "rsa" ; }
  @Override
  public String    getKeyMode() {   return _mode ; }
  public String    toString(){
    StringBuffer sb = new StringBuffer() ;
    if( _mode.equals( "public" ) ){
       sb.append( " ---  Rsa Public Key -- for " ) ;
       if( _domainList != null ) {
           for (int i = 0; i < _domainList.length; i++) {
               sb.append(_domainList[i]);
           }
       }
       sb.append( "\n" ) ;
       sb.append( " bit length = "+_n.bitLength()+"\n" ) ;
       sb.append( "          n = "+_n.toString(16)+"\n" ) ;
       sb.append( "          e = "+_e.toString(16)+"\n" ) ;
    }else if( _mode.equals( "private" ) ){
       sb.append( " ---  Rsa Private Key -- for " ) ;
       if( _domainList != null ) {
           for (int i = 0; i < _domainList.length; i++) {
               sb.append(_domainList[i]);
           }
       }
       sb.append( "\n" ) ;
       sb.append( " bit length = "+_n.bitLength()+"\n" ) ;
       sb.append( " n = "+_n.toString(16)+"\n" ) ;
       sb.append( " d = "+_e.toString(16)+"\n" ) ;
    }
    return sb.toString();
  }
}
