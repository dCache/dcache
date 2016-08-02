package dmg.cells.services.login ;

import dmg.util.cdb.CdbContainable;
import dmg.util.cdb.CdbElementable;
import dmg.util.cdb.CdbFileRecordHandle;
import dmg.util.cdb.CdbLockable;

/**
  *   <table border=1>
  *   <tr><th>Key</th><th>Meaning</th></tr>
  *   <tr><td>e-mail</td><td>preferred e-mail address</td></tr>
  *   <tr><td>password</td><td>md5 encrypted password</td></tr>
  *   <tr><td>allowed</td><td>list of allowed attributes</td></tr>
  *   <tr><td>denied</td><td>list of denied attributes</td></tr>
  *   </table>
  */
public class  UserHandle extends CdbFileRecordHandle {
   private CdbContainable _container ;
   private String         _name ;
   public UserHandle( String  name ,
                        CdbContainable container  ,
                        CdbElementable element ){

        super( name , container , element ) ;
        _container = container ;
        _name      = name ;
   }
   public void setEmail( String name ){
      setAttribute( "e-mail" , name ) ;
   }
   public void setPassword( String password ){
      setAttribute( "password" , password ) ;
   }
   public void setAllowed( String [] allowedList ){
      setAttribute( "allowed" , allowedList ) ;
   }
   public void setDenied( String [] deniedList ){
      setAttribute( "denied" , deniedList ) ;
   }
   public void addParent( String parent ){
      addListItem( "parents" , parent , true ) ;
   }
   public String [] getParents(){
      return (String[])getAttribute("parents") ;
   }
   public String [] getChilds(){
      return (String[])getAttribute("childs") ;
   }
   public void removeParent( String parent ){
      removeListItem( "parents" , parent ) ;
   }
   public void addChild( String child ){
      addListItem( "childs" , child , true ) ;
   }
   public void removeChild( String child ){
      removeListItem( "childs" , child ) ;
   }
   public UserPrivileges getUserPrivileges(){
      return
      new UserPrivileges( _name ,
                          (String[])getAttribute("allowed") ,
                          (String[])getAttribute("denied" ) ) ;
   }
   public void addAllowed( String allowed ){
      addListItem( "allowed" , allowed , true ) ;
      removeListItem( "denied" , allowed ) ;
   }
   public void removeAllowed( String allowed ){
      removeListItem( "allowed" , allowed ) ;
   }
   public void addDenied( String denied ){
      addListItem( "denied" , denied , true ) ;
      removeListItem( "allowed" , denied ) ;
   }
   public void removeDenied( String denied ){
      removeListItem( "denied" , denied ) ;
   }
   public String getPassword(){
      return (String)getAttribute("password") ;
   }
   public String getEmail(){
      return (String)getAttribute("e-mail") ;
   }
   public String getMode(){
      return (String)getAttribute("mode") ;
   }
   public boolean isGroup(){ return getMode().equals("group") ; }
   public String [] getAllowed(){
      return (String[])getAttribute("allowed") ;
   }
   public String [] getDenied(){
      return (String[])getAttribute("allowed") ;
   }
   public String toString(){
      StringBuilder sb = new StringBuilder() ;
      try{
         open( CdbLockable.READ ) ;
            if( isGroup() ){
               sb.append("Group   : ").append(getName()).append('\n');
               String [] childs = getChilds() ;
               if( childs.length > 0 ){
                  sb.append( "Members : \n" ) ;
                   for (String child : childs) {
                       sb.append("          ").append(child).append('\n');
                   }
               }
            }else{
               sb.append("User    : ").append(getName()).append('\n');
            }
            sb.append("e-mail  : ").append(getEmail()).append('\n');
            sb.append("passwd  : ").append(getPassword()).append('\n');
            String [] parents = getParents() ;
            if( parents.length > 0 ){
               sb.append( "Parents : \n" ) ;
                for (String parent : parents) {
                    sb.append("          ").append(parent).append('\n');
                }
            }
            sb.append( "prives  : \n" ) ;
            sb.append(getUserPrivileges()) ;
         close( CdbLockable.COMMIT ) ;
      }catch( Exception e ){
         sb.append("PANIC : Exception ").append(e).append('\n');
      }
      return sb.toString() ;
   }
}
