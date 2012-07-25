//
// $Id: CVPacket.java,v 1.1 2002-03-18 09:04:44 cvs Exp $
//
package dmg.cells.services.codeVersioning ;


public class CVPacket implements java.io.Serializable {

   private int    _resultCode;
   private String _resultMessage   = "" ;
   private int    _sessionId       = -1 ;
   private int    _packetId        = -1 ;
   
   public CVPacket( CVPacket packet ){
      _sessionId = packet._sessionId ;
      _resultCode = 0 ;
      _resultMessage = "" ;
   }
   public CVPacket(){}
   public void setResult( int code , String message ){
      _resultCode    = code ;
      _resultMessage = message ;
   }
   public int getResultCode(){ return _resultCode ; }
   public String getResultMessage(){ return _resultMessage ; }
   public void setSessionId( int sessionId ){ 
     _sessionId = sessionId ;
   }
   public void setPacketId( int packetId ){
     _packetId = packetId ;
   }
   public int getSessionId(){ return _sessionId ; }
   public int getPacketId(){ return _packetId ; }
   

}
