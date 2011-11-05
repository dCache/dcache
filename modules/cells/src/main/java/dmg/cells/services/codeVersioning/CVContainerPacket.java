//
// $Id: CVContainerPacket.java,v 1.1 2002-03-18 09:04:44 cvs Exp $
//
package dmg.cells.services.codeVersioning ;


public class CVContainerPacket extends CVPacket {

   private String _name = null ;
   private String _type = null ;
   public CVContainerPacket( String name , String type ){
      _name = name ;
      _type = type ;
   }
   public String getName(){ return _name ; }
   public String getType(){ return _type ; }

}
