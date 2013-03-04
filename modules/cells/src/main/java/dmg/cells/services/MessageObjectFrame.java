package dmg.cells.services ;

import java.io.Serializable;

import dmg.cells.nucleus.CellPath;

/**
  *
  *
  * @author Patrick Fuhrmann
  * @version 0.1, 15 Feb 1998
  */
public class      MessageObjectFrame
       implements Serializable       {

   private static final long serialVersionUID = -1127590849911644109L;
   private int      _id;

   private CellPath _path;
   private Object   _obj;
   public MessageObjectFrame( int id , CellPath path , Serializable obj ){
     _id   = id ;
     _path = path ;
     _obj  = obj ;
  }
   public int      getId(){        return _id ; }
   public CellPath getCellPath() { return _path ; }
   public Serializable getObject()   { return (Serializable) _obj ; }
   public void     setObject( Serializable obj ){ _obj = obj ; }
   public boolean equals( Object o ){
     return ( o instanceof MessageObjectFrame    ) &&
            ( ((MessageObjectFrame)o)._id == _id )   ;
   }
   public int hashCode(){ return _id ; }
}
