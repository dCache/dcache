// $Id: UserMetaDb.java,v 1.2 2002-02-19 20:39:06 cvs Exp $
package dmg.cells.services.login.user  ;
import java.io.* ;
import java.util.* ;

public class UserMetaDb {

   private class UserMetaItem implements UserMetaDictionary {
       private String    _name;
       private Hashtable _attr    = new Hashtable() ;
       private UserMetaItem( String name ){ _name = name ; }
       private void addAttribute( String key , String value ){
           _attr.put( key , value ) ;
       }
       private Enumeration getAttributes(){ return _attr.keys() ; }
       private void removeAttribute(String key ){
          _attr.remove( key ) ;
       }
       private String getAttribute(String key){

           String attr = (String)_attr.get(key) ;
           if( attr == null ) {
               return "<notSet>";
           }
           return attr ;
       }
       @Override
       public Enumeration keys(){ return _attr.keys() ; }
       @Override
       public String valueOf(String key){
          return (String)_attr.get(key) ;
       }
   }
   private File      _userMetaDir;
   private AgingHash _hash        = new AgingHash(20) ;
   public UserMetaDb( File userMetaDir ){
      if( ! userMetaDir.isDirectory() ) {
          throw new
                  IllegalArgumentException("Not a user meta DB(not a dir)" + userMetaDir);
      }
      _userMetaDir = userMetaDir;
   }
   public synchronized boolean isGroup(String principalName ){
       UserMetaItem item = null ;
       try{
         item = getUser( principalName ) ;
       }catch(Exception ii ){
          return false ;
       }
       String type = item.getAttribute("type") ;
       if( type == null ) {
           return false;
       }
       return type.equals("group") ;
   }
   public synchronized boolean exists(String principalName ){
       UserMetaItem item = null ;
       try{
         item = getUser( principalName ) ;
       }catch(Exception ii ){
          return false ;
       }
       String type = item.getAttribute("type") ;
       if( type == null ) {
           return false;
       }
       return true ;
   }
   private void putUser( String userName , UserMetaItem item )
           throws DatabaseException{
       _storeUser( userName , item ) ;
       _hash.put( userName , item ) ;
       return ;
   }
   private UserMetaItem getUser( String userName )
           throws NoSuchElementException{
       UserMetaItem item = (UserMetaItem)_hash.get( userName ) ;
       if( item != null ) {
           return item;
       }
       return _loadUser( userName ) ;
   }
   private void _storeUser( String userName , UserMetaItem item )
           throws DatabaseException {

      File file = new File( _userMetaDir , "."+userName ) ;
      PrintWriter pw = null ;
      try{
           pw = new PrintWriter(
                    new FileWriter( file ) ) ;
      }catch(IOException ioe ){
          throw new
          DatabaseException( "Can't create : "+userName ) ;
      }
      Enumeration e = item.getAttributes() ;
      while( e.hasMoreElements() ){
          String key = (String)e.nextElement() ;
          pw.println(key+"="+item.getAttribute(key)) ;
      }
      pw.close() ;
      file.renameTo( new File( _userMetaDir , userName ) ) ;
      return ;
   }
   private UserMetaItem _loadUser( String userName )
           throws NoSuchElementException {

       File file = new File( _userMetaDir , userName ) ;
       if( ! file.exists() ) {
           throw new
                   NoSuchElementException("User not found : " + userName);
       }

        BufferedReader br   = null ;

        try{
          br = new BufferedReader( new FileReader( file ) ) ;
        }catch( IOException e ){
           throw new
           NoSuchElementException( "No found "+file ) ;
        }
        String line = null ;
        StringTokenizer st   = null ;
        UserMetaItem    item = new UserMetaItem( userName ) ;
        String key = null , value = null ;
        try{
           while( ( line = br.readLine() ) != null ){
              st     = new StringTokenizer( line , "=" ) ;
              key   = st.nextToken() ;
              value = st.nextToken() ;
              item.addAttribute( key , value ) ;
           }
        }catch(NoSuchElementException nsee ){
           throw new
           NoSuchElementException( "Syntax error in "+file ) ;
        }catch(IOException ioe ){
           throw new
           NoSuchElementException( "IOError on "+file ) ;
        }catch(Exception ee ){
           throw new
           NoSuchElementException( "IOError on "+file ) ;
        }finally{
            try{ br.close() ; }catch(Exception ee){}
        }
        return item ;
   }
   public synchronized void createUser( String userName )
          throws DatabaseException {

       try{
          getUser( userName ) ;
       }catch(Exception ii){
          UserMetaItem item = new UserMetaItem(userName) ;
          item.addAttribute( "type" , "user" ) ;
          putUser( userName , item ) ;
          return ;
       }
       throw new
       DatabaseException( "Already exists : "+userName ) ;

   }
   public synchronized void createGroup( String groupName )
          throws DatabaseException {

       try{
          getUser( groupName ) ;
       }catch(Exception ii){
          UserMetaItem item = new UserMetaItem(groupName) ;
          item.addAttribute( "type" , "group" ) ;
          putUser( groupName , item ) ;
          return ;
       }
       throw new
       DatabaseException( "Already exists : "+groupName ) ;

   }
   public synchronized void removePrincipal( String principalName )
          throws NoSuchElementException {
      _hash.remove( principalName ) ;
      boolean ok = new File( _userMetaDir , principalName ).delete() ;
      if( ! ok ) {
          throw new
                  NoSuchElementException("Not found : " + principalName);
      }
   }
   public synchronized void setAttribute( String principalName ,
                                          String key ,
                                          String value )
          throws DatabaseException {
       UserMetaItem item = getUser( principalName ) ;
       item.addAttribute( key , value ) ;
       putUser( principalName , item ) ;
       return ;
   }
   public synchronized UserMetaDictionary getDictionary( String principalName )
          throws NoSuchElementException {

       return getUser( principalName ) ;
   }
   public synchronized void removeAttribute( String principalName , String key )
          throws DatabaseException {

       UserMetaItem item = getUser( principalName ) ;
       item.removeAttribute( key ) ;
       putUser( principalName , item ) ;
       return ;
   }
}

