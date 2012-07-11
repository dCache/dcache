package dmg.cells.applets.login ;

import java.io.* ;

public class DomainObjectFrame implements Serializable {

   static final long serialVersionUID = -1845956304286394450L;

   private Object _payload ;
   private String _destination = null ;
   private int _id , _subId ;
   public DomainObjectFrame( Object payload ,
                             int id ,
                             int subId            ){	
        _payload = payload ;
        _id      = id ;
        _subId   = subId ;
   }
   public DomainObjectFrame( String destination ,
                             Object payload ,
                             int id ,
                             int subId            ){	
      this( payload , id , subId ) ;
      _destination = destination ;
   }
   public void setPayload( Object payload ){ _payload = payload ; }
   public int getId(){ return _id ; }
   public int getSubId(){ return _subId ; }
   public Object getPayload(){ return _payload ; }
   public String getDestination(){ return _destination ; }
   public int hashCode(){ return _id ; }
   public boolean equals( Object obj ){
      if( ! ( obj instanceof DomainObjectFrame ) ) {
          return false;
      }
      return ((DomainObjectFrame)obj)._id == _id ;
   }
}
