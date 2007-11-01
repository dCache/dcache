package dmg.apps.osm ;

import java.util.* ;
import java.io.* ;
import java.text.* ;

 public  class DriveInfo implements Serializable{
   private String _drive , _status , _owner , _tape ;
   DriveInfo( String drive , 
              String status , 
              String tape ,
              String owner ){
              
     _drive  = drive  ;  
     _status = status ;
     _tape   = tape ;
     _owner  = owner ;
   }
   public String getDriveName(){ return _drive ; }
   public String getStatus(){ return _status ; }
   public String getTape(){ return _tape ; }
   public String getOwner(){ return _owner ; }
   public String toString(){
      return "d="+_drive+";s="+_status+";t="+_tape+";o="+_owner ;
   }
   public boolean equals( Object o ){
      if( ! ( o instanceof DriveInfo) )return false ;
      DriveInfo x = (DriveInfo) o ;
      return x._drive.equals(_drive) &&
             x._status.equals(_status) &&
             x._tape.equals(_tape) &&
             x._owner.equals(_owner) ;
   }
}
