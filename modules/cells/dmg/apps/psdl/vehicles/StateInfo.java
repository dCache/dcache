package dmg.apps.psdl.vehicles ;

import  java.io.* ;

/**
  *  
  *
  * @author Patrick Fuhrmann
  * @version 0.1, 15 Feb 1998
  */
public class  StateInfo implements Serializable {  
   private String  _name ;
   private boolean _up  ;
   private boolean _active ;
   private int     _period = 0 ;
   public StateInfo( String name , boolean state ){
       _name   = name ;
       _up     = state ;
       _active = true ;
   }
   public String  getName(){ return _name ; }
   public boolean isUp(){ return _up ; }
   public String  toString(){
      return _name+"="+
             (_up?"(Up)":"(Down)")+(_active?"(Active)":"(Inactive)") ; }
   public boolean isActive(){ return _active ; }
   public void    setActive( boolean active ){ _active = active ; }
   public void    setPeriod( int period ){ _period = period ; }
   public int     getPeriod(){ return _period ; }
}
