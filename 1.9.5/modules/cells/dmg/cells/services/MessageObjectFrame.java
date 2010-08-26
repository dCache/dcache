package dmg.cells.services ;

import java.io.* ;
import dmg.cells.nucleus.* ;

/**
  *  
  *
  * @author Patrick Fuhrmann
  * @version 0.1, 15 Feb 1998
  */
public class      MessageObjectFrame 
       implements Serializable       {

   static final long serialVersionUID = -1127590849911644109L;
   private int      _id = 0 ;
   
   private CellPath _path = null ;
   private Object   _obj  = null ;
   public MessageObjectFrame( int id , CellPath path , Object obj ){
     _id   = id ;
     _path = path ;
     _obj  = obj ;
  }
   public int      getId(){        return _id ; }
   public CellPath getCellPath() { return _path ; }
   public Object   getObject()   { return _obj ; }
   public void     setObject( Object obj ){ _obj = obj ; }
   public boolean equals( Object o ){
     return ( o instanceof MessageObjectFrame    ) &&
            ( ((MessageObjectFrame)o)._id == _id )   ;
   }
   public int hashCode(){ return _id ; }
}
