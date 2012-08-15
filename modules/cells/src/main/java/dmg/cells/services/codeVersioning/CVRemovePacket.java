//
// $Id: CVRemovePacket.java,v 1.1 2002-03-18 09:04:44 cvs Exp $
//
package dmg.cells.services.codeVersioning ;


public class CVRemovePacket extends CVContainerPacket {

    private static final long serialVersionUID = -8907868913246909290L;

    public CVRemovePacket( String name , String type ){
      super(name,type);
   } 
}
