package dmg.security.cipher.rsa ;

import java.math.BigInteger;

import dmg.security.cipher.EncryptionKey;

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
      System.arraycopy(domainList, 0, _domainList, 0, domainList.length);


  }
  @Override
  public String [] getDomainList(){ return _domainList ; }
  @Override
  public String    getKeyType() {   return "rsa" ; }
  @Override
  public String    getKeyMode() {   return _mode ; }
  public String    toString(){
    StringBuilder sb = new StringBuilder() ;
    if( _mode.equals( "public" ) ){
       sb.append( " ---  Rsa Public Key -- for " ) ;
       if( _domainList != null ) {
           for (String domain : _domainList) {
               sb.append(domain);
           }
       }
       sb.append( "\n" ) ;
       sb.append(" bit length = ").append(_n.bitLength()).append("\n");
       sb.append("          n = ").append(_n.toString(16)).append("\n");
       sb.append("          e = ").append(_e.toString(16)).append("\n");
    }else if( _mode.equals( "private" ) ){
       sb.append( " ---  Rsa Private Key -- for " ) ;
       if( _domainList != null ) {
           for (String domain : _domainList) {
               sb.append(domain);
           }
       }
       sb.append( "\n" ) ;
       sb.append(" bit length = ").append(_n.bitLength()).append("\n");
       sb.append(" n = ").append(_n.toString(16)).append("\n");
       sb.append(" d = ").append(_e.toString(16)).append("\n");
    }
    return sb.toString();
  }
}
