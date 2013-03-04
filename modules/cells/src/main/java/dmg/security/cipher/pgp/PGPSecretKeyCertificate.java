 package dmg.security.cipher.pgp ;

 import java.math.BigInteger;

/**
  *
  *
  * @author Patrick Fuhrmann
  * @version 0.1, 15 Feb 1998
  */
 public class PGPSecretKeyCertificate extends PGPKeyCertificate {
      private BigInteger _d , _q , _p , _u ;
      public PGPSecretKeyCertificate( int ctb , int v ,
                                         int t   , int val , int al){
        super( ctb , v , t , val , al ) ;
      }
      public void setPrivate( BigInteger d , BigInteger p ,
                              BigInteger q , BigInteger u   ){
         _d = d ;
         _p = p ;
         _q = q ;
         _u = u ;
      }
      public BigInteger getD(){ return _d ; }
      public BigInteger getP(){ return _p ; }
      public BigInteger getQ(){ return _q ; }

   public String toString(){
      StringBuilder sb = new StringBuilder() ;
      sb.append( super.toString() ) ;
      sb.append(" p        = ").append(_p.toString(16)).append("\n");
      sb.append(" q        = ").append(_q.toString(16)).append("\n");
      sb.append(" d        = ").append(_d.toString(16)).append("\n");
      return sb.toString() ;

   }


 }
