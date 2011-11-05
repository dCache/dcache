package dmg.security.cipher.pgp ;
import java.math.BigInteger ;
 
/**
  *  
  *
  * @author Patrick Fuhrmann
  * @version 0.1, 15 Feb 1998
  */
 public class PGPKeyCertificate extends PGPPacket {
      private int _version ;
      private int _timestamp ;
      private int _validity ;
      private int _publicAlgorithm ;
 
      BigInteger _n , _e ;
      
      public PGPKeyCertificate( int ctb , int v , int t , int val , int al){
        super( ctb ) ;
        _version         = v ;
        _timestamp       = t ;
        _validity        = val ;
        _publicAlgorithm = al ;
      
      } 
      public void setPublic( BigInteger n , BigInteger e ){
         _n = n ;
         _e = e ;
      }
      public BigInteger getN(){ return _n ; }
      public BigInteger getE(){ return _e ; }
      public String toString(){
      StringBuffer sb = new StringBuffer() ;      
         sb.append( " Version  = "+_version+"\n" ) ;
         sb.append( " Validity = "+_validity+"\n" ) ;
         sb.append( " e        = "+_e.toString(16)+"\n" ) ;
         sb.append( " n        = "+_n.toString(16)+"\n" ) ;
         return sb.toString() ;
      }
 }
