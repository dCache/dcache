// $Id: LoginBroker.java,v 1.3.2.2 2006-12-06 10:11:47 tigran Exp $

package dmg.cells.services.login ;

import java.util.* ;
import dmg.cells.nucleus.*;
import dmg.util.* ;

public class LoginBroker 
       extends  CellAdapter
       implements Runnable  {

  private CellNucleus  _nucleus ;
  private Args         _args ;

  private int _delay = 3 ;
  
  private HashMap _hash = new HashMap() ;
  
  private class LoginEntry {
     private long _time ;
     private LoginBrokerInfo _info ;
     private LoginEntry( LoginBrokerInfo info ){
        _time = System.currentTimeMillis() ;
        _info = info ;
     }
     private boolean isValid(){ 
        return System.currentTimeMillis() <
               ( _time + (long)_delay * _info.getUpdateTime() ) ;
     }
     public LoginBrokerInfo getInfo(){ return _info ; }
     public boolean equals( Object obj ){
        return (obj instanceof LoginEntry ) && _info.equals( ((LoginEntry)obj).getInfo() ) ;
     }
     public int hashCode(){ return _info.hashCode() ; }
     public String toString(){
        return _info.toString()+
              (_time+_info.getUpdateTime()-System.currentTimeMillis())+
              ";"+
              (isValid()?"VALID":"INVALID")+";" ;
     }
     public String getIdentifier(){ return _info.getIdentifier() ; }
  }
  public LoginBroker( String name , String argString ) throws Exception {
  
      super( name , argString , false ) ;

      _nucleus  = getNucleus() ;
      _args     = getArgs() ;
      
      _nucleus.newThread(this,"Cleaner").start() ;
      start() ;
  }
  public CellVersion getCellVersion(){ 
      return new CellVersion(super.getCellVersion().getRelease(),"$Revision: 1.3.2.2 $" ); 
  }
  public String hh_ls = "[-binary] [-protocol=<protocol>] [-time]" ;
  public Object ac_ls( Args args ){
  
     boolean   binary   = args.getOpt("binary") != null ;
     String    protocol = args.getOpt("protocol") ;
     ArrayList list     = new ArrayList() ;
     StringBuffer sb    = new StringBuffer() ;
     boolean   showTime = args.getOpt("l") != null ;
     
     synchronized( this ){
        Iterator i = _hash.values().iterator() ;
        while( i.hasNext() ){
           LoginEntry      entry = (LoginEntry)i.next() ;
           LoginBrokerInfo info  = entry.getInfo() ;
           if( ( protocol != null ) &&
               ( ! protocol.equals( info.getProtocolFamily() ) ) )continue;
           if( binary )list.add( info ) ;
           else{
              sb.append( showTime ? entry.toString() : info.toString() ) ;
              sb.append("\n");
           }
        }
     }
     if( binary ){
        return list.toArray( new  LoginBrokerInfo[list.size()] ) ;
     }else{
        return sb.toString() ;
     }
  
  }
  public void run(){
     try{
        while( ! Thread.interrupted() ){
          synchronized(this){
             HashMap set = new HashMap() ;
             
             Iterator i = _hash.values().iterator() ;
             while( i.hasNext() ){
                LoginEntry entry = (LoginEntry)i.next() ;
                if( entry.isValid() )set.put(entry.getIdentifier(),entry) ;
             }
             _hash = set ;
          }
          Thread.sleep(60000);
        }
     }catch(Exception ee ){
        say("Worker interrupted due to : "+ee ) ;
     }
  }  
  public void messageArrived( CellMessage msg ){
     Object obj = msg.getMessageObject() ;
     
     if( obj instanceof LoginBrokerInfo ){
        synchronized(this){
           LoginEntry entry = new LoginEntry((LoginBrokerInfo)obj) ;
           _hash.put( entry.getIdentifier() , entry ) ;
        }
     }
  }
  

}
