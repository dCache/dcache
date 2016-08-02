// $Id: UserMetaDb.java,v 1.2 2002-02-19 20:39:06 cvs Exp $
package dmg.cells.services.login.user  ;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.NoSuchElementException;
import java.util.StringTokenizer;

public class UserMetaDb {

   private static class UserMetaItem implements UserMetaDictionary {
       private String    _name;
       private Hashtable<String, String> _attr    = new Hashtable<>() ;
       private UserMetaItem( String name ){ _name = name ; }
       private void addAttribute( String key , String value ){
           _attr.put( key , value ) ;
       }
       private Enumeration<String> getAttributes(){ return _attr.keys() ; }
       private void removeAttribute(String key ){
          _attr.remove( key ) ;
       }
       private String getAttribute(String key){

           String attr = _attr.get(key);
           if( attr == null ) {
               return "<notSet>";
           }
           return attr ;
       }
       @Override
       public Enumeration<String> keys(){ return _attr.keys() ; }
       @Override
       public String valueOf(String key){
          return _attr.get(key);
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
       UserMetaItem item;
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
       UserMetaItem item;
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

      File file = new File(_userMetaDir , '.' + userName ) ;
      PrintWriter pw;
      try{
           pw = new PrintWriter(
                    new FileWriter( file ) ) ;
      }catch(IOException ioe ){
          throw new
          DatabaseException( "Can't create : "+userName ) ;
      }
      Enumeration<String> e = item.getAttributes() ;
      while( e.hasMoreElements() ){
          String key = e.nextElement();
          pw.println(key + '=' + item.getAttribute(key)) ;
      }
      pw.close() ;
      file.renameTo( new File( _userMetaDir , userName ) ) ;
   }
   private UserMetaItem _loadUser( String userName )
           throws NoSuchElementException {

       File file = new File( _userMetaDir , userName ) ;
       if( ! file.exists() ) {
           throw new
                   NoSuchElementException("User not found : " + userName);
       }

        BufferedReader br;

        try{
          br = new BufferedReader( new FileReader( file ) ) ;
        }catch( IOException e ){
           throw new
           NoSuchElementException( "No found "+file ) ;
        }
        String line;
        StringTokenizer st;
        UserMetaItem    item = new UserMetaItem(userName);
        String key, value;
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
        }catch(Exception ioe ){
           throw new
           NoSuchElementException( "IOError on "+file ) ;
        } finally{
            try{ br.close() ; }catch(Exception ee){}
        }
        return item ;
   }
   public synchronized void createUser( String userName )
          throws DatabaseException {

       try{
          getUser( userName ) ;
       }catch(Exception ii){
          UserMetaItem item = new UserMetaItem(userName);
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
          UserMetaItem item = new UserMetaItem(groupName);
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
   }
}

