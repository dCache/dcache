//
// $Id: CVCreatePacket.java,v 1.1 2002-03-18 09:04:44 cvs Exp $
//
package dmg.cells.services.codeVersioning ;


public class CVCreatePacket extends CVContainerPacket {

   private boolean _truncate  = true ;
   private boolean _exclusive = true ;
   public CVCreatePacket( String name , String type ){
      super(name,type);
   } 
   
   public CVCreatePacket( String name , String type , 
                          boolean truncate , boolean exclusive ){
      super(name,type) ;
      _truncate  = truncate ;
      _exclusive = exclusive ;
   }
   public boolean isTruncate(){ return _truncate ; }
   public boolean isExclusive(){ return _exclusive ; }

}
