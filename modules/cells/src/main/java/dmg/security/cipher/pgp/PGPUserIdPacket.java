package dmg.security.cipher.pgp ;


/**
  *  
  *
  * @author Patrick Fuhrmann
  * @version 0.1, 15 Feb 1998
  */
public class PGPUserIdPacket extends PGPPacket {

   String _id;
   public PGPUserIdPacket( int ctb , String id ){
      super( ctb ) ;
      _id = id ;
   }
   public String getId(){ return _id ; }
   public String toString(){
   
      return "\n User Id Packet of : "+_id +"\n" ;
   }

}
