// $Id: AclDb.java,v 1.4 2006-12-15 10:58:14 tigran Exp $
package dmg.cells.services.login.user  ;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.NoSuchElementException;
import java.util.StringTokenizer;
import java.util.Vector;

public class AclDb {

   private static class AclItem implements AcDictionary {
       private String    _name;
       private Hashtable<String, Boolean> _users    = new Hashtable<>() ;
       private String    _inherits;
       private AclItem( String name ){ _name = name ; }
       private void setInheritance( String aclItem ){
           _inherits = aclItem ;
       }
       private void addAccess( String user , boolean access ){
           _users.put( user , access) ;
       }
       private Enumeration<String> getUsers(){ return _users.keys() ; }
       private void removeUser(String user ){
          _users.remove( user ) ;
       }
       private Boolean getUserAccess(String user)
               throws NoSuchElementException{
           return _users.get(user);
       }
       private void merge( String user , Boolean access ){
           if( _users.get( user ) == null ) {
               _users.put(user, access);
           }
       }
       //
       // the AcDictionary interface
       //
       @Override
       public Enumeration<String> getPrincipals(){
           return _users.keys() ;
       }
       @Override
       public boolean getPermission( String principal ){
           Boolean ok = _users.get(principal);
           if( ok == null ) {
               throw new NoSuchElementException(principal);
           }
           return ok;
       }
       @Override
       public boolean isResolved(){ return _inherits == null ; }
       @Override
       public String getInheritance(){ return _inherits ; }
       public AclItem cloneMe(){
           AclItem item   = new AclItem(_name);
           item._users    = (Hashtable<String, Boolean>)_users.clone() ;
           item._inherits = _inherits ;
           return item ;
       }
   }
   private File      _aclDir;
   private AgingHash _hash   = new AgingHash(20) ;
   public AclDb( File aclDir ){
      if( ! aclDir.isDirectory() ) {
          throw new
                  IllegalArgumentException("Not a acl DB(not a dir)" + "aclDir \"" + aclDir
                  .getPath() + ' ' + aclDir.getName() + '"');
      }
      _aclDir = aclDir ;
   }
   private void putAcl( String aclName , AclItem item )
           throws DatabaseException{
       _storeAcl( aclName , item ) ;
       _hash.put( aclName , item ) ;
   }
   private AclItem getAcl( String aclName )
           throws NoSuchElementException{
//       System.out.println( "getAcl : "+aclName) ;
       AclItem item = (AclItem)_hash.get( aclName ) ;
       if( item != null ) {
           return item.cloneMe();
       }
       return _loadAcl( aclName ) ;
   }
   private void _storeAcl( String aclName , AclItem item )
           throws DatabaseException {

       File file = new File(_aclDir , '.' + aclName ) ;
       PrintWriter pw;
       try{
           pw = new PrintWriter(
                    new FileWriter( file ) ) ;
      }catch(IOException ioe ){
          throw new
          DatabaseException( "Can't create : "+aclName ) ;
      }
      String inherit = item.getInheritance() ;
      if( inherit != null ) {
          pw.println("$=" + inherit);
      }
      Enumeration<String> e = item.getUsers() ;
      while( e.hasMoreElements() ){
          String user = e.nextElement();
          Boolean access = item.getUserAccess(user) ;
          pw.println(user + '=' + (access ? "allowed" : "denied")) ;
      }
      pw.close() ;
      file.renameTo( new File( _aclDir , aclName ) ) ;
   }
   private AclItem _loadAcl( String aclName )
           throws NoSuchElementException
   {
       File file = new File(_aclDir, aclName);
       if (!file.exists()) {
           throw new NoSuchElementException("Acl  not found : " + aclName);
       }

       AclItem item = new AclItem(aclName);

       try (BufferedReader br = new BufferedReader(new FileReader(file))) {
           String line;
           while ((line = br.readLine()) != null) {
               StringTokenizer st = new StringTokenizer(line, "=");
               String user = st.nextToken();
               String access = st.nextToken();
               if (user.equals("$")) {
                   item.setInheritance(access);
               } else if (access.equals("allowed")) {
                   item.addAccess(user, true);
               } else if (access.equals("denied")) {
                   item.addAccess(user, false);
               }
           }
       } catch (FileNotFoundException e) {
           throw new NoSuchElementException("Not found " + file);
       } catch (NoSuchElementException e) {
           throw new NoSuchElementException("Syntax error in " + file);
       } catch (Exception e) {
           throw new NoSuchElementException("IOError on " + file);
       }
       return item;
   }
   public synchronized void createAclItem( String aclItem )
          throws DatabaseException {
       putAcl(aclItem , new AclItem(aclItem)) ;
   }
   public synchronized void removeAclItem( String aclItem )
   {
      _hash.remove( aclItem ) ;
      new File( _aclDir , aclItem ).delete() ;
   }
   public synchronized void setInheritance( String aclItem , String inheritsFrom)
          throws DatabaseException {
       AclItem item = getAcl( aclItem ) ;
       item.setInheritance( inheritsFrom ) ;
       putAcl( aclItem , item ) ;
   }
   public synchronized void addAllowed( String aclItem , String user )
          throws DatabaseException {
       AclItem item = getAcl( aclItem ) ;
       item.addAccess( user , true ) ;
       putAcl( aclItem , item ) ;
   }
   public synchronized void addDenied( String aclItem , String user )
          throws DatabaseException {

       AclItem item = getAcl( aclItem ) ;
       item.addAccess( user , false ) ;
       putAcl( aclItem , item ) ;
   }
   public synchronized void removeUser( String aclItem , String user )
          throws DatabaseException {

       AclItem item = getAcl( aclItem ) ;
       item.removeUser( user ) ;
       putAcl( aclItem , item ) ;
   }
   public AcDictionary getPermissions( String aclName , boolean resolve )
          throws NoSuchElementException{

//        return resolve ? _resolveAclItem( aclName ) : getAcl( aclName ) ;
        return resolve ? _fullResolveAclItem( aclName ) : getAcl( aclName ) ;

   }
   private AclItem _fullResolveAclItem( String aclItem )
           throws NoSuchElementException{

       if( aclItem.startsWith("." ) || aclItem.endsWith("." ) ) {
           throw new
                   NoSuchElementException("Illegal formated acl : " + aclItem);
       }
       int f = aclItem.indexOf('.') ;
       int l = aclItem.lastIndexOf('.');
       String [] array;
       if( f < 0 ){
         array = new String[2] ;
         array[0] = aclItem ;
         array[1] = "*.*.*" ;
       }else if( f == l ){
         String a = aclItem.substring(0,f) ;
         String e = aclItem.substring(l+1) ;
         array = new String[4] ;
         array[0] = aclItem ;
         array[1] = a + '.' + e + ".*" ;
         array[2] = a+".*.*" ;
         array[3] = "*.*.*" ;
       }else if( ( f + 1 ) == l ){
           throw new
           NoSuchElementException("Illegal formated acl : "+aclItem ) ;
       }else{
         String className    = aclItem.substring(0,f) ;
         String instanceName = aclItem.substring(f+1,l) ;
         String actionName   = aclItem.substring(l+1) ;
         //
         // split the  object-instance-name into all possiblities.
         // What we call 'possibilities' is : filling the
         // items with stars from left to right.
         //
         //  e.g. : a.b.c -> V(a.b.c)={a.b.c,a.b.*,a.*.*,*.*.*}
         //  formal :
         //     V(a[0].a[1]...a[n])[0] = a[0].a[1]...a[n-1].*
         //
         StringTokenizer st = new StringTokenizer(instanceName,".") ;
         String [] mm = new String[st.countTokens()] ;
         int firstStar = mm.length ;
         for(int i = 0 ; i < mm.length ; i++ ){
            mm[i] = st.nextToken() ;
            if( ( firstStar == mm.length ) &&
                  mm[i].equals("*")              ) {
                firstStar = i;
            }
         }
//         System.out.println( "getAcl : fistStar = "+firstStar);
         String [] mask = new String[firstStar+1] ;
         int k = 0 ;
         for( int i = firstStar ; i >= 0 ; i-- ){
            StringBuilder sb = new StringBuilder() ;
            for( int j = 0 ; j < mm.length ; j++ ){
               if( j >= i ) {
                   sb.append("*.");
               } else {
                   sb.append(mm[j]).append('.');
               }
            }
            mask[k++] = sb.toString() ;
//            System.out.println( "getAcl : mask[k="+(k-1)+"] = "+mask[k-1]);
         }
         //
         //   I don't know if this is what we actually want :
         //   we walk through all possiblities but the order might
         //   be wrong or at least worth a discussion.
         //
         //      class.V(instance)[0].action
         //      class.V(instance)[0].*
         //      class.V(instance)[1].action
         //      class.V(instance)[1].*
         //      class.*.*
         //      *.*.*
         //
         k = 0 ;
         if( className.equals("*") ){
            array = new String[1] ;
         }else if( actionName.equals("*") && instanceName.equals("*") ){
            array = new String[2] ;
            array[k++] = className+".*.*" ;
         }else if( actionName.equals("*") ){
            array = new String[2+mask.length] ;
             for (String s : mask) {
                 array[k++] = className + '.' + s + '*';
             }
            array[k++] = className+".*.*" ;
         }else if( instanceName.equals("*") ){
            array = new String[3] ;
            array[k++] = className+".*."+actionName ;
            array[k++] = className+".*.*" ;
         }else{
            array = new String[2+2*mask.length] ;
             for (String s : mask) {
                 array[k++] = className + '.' + s + actionName;
                 array[k++] = className + '.' + s + '*';
             }
            array[k++] = className+".*.*" ;
         }
         array[k++] = "*.*.*" ;
       }
       //
       // array contains all the possiblities we have to merge
       // before we check them against the relation database.
       //
       AclItem currentItem = null ;
       for (String s : array) {
           if (currentItem == null) {
               try {
                   currentItem = _resolveAclItem(s);
               } catch (NoSuchElementException ee) {
                   // no problem;
               }
           } else {
               try {
                   AclItem nextItem = _resolveAclItem(s);
                   Enumeration<String> e = nextItem.getUsers();
                   while (e.hasMoreElements()) {
                       String user = e.nextElement();
                       currentItem.merge(user, nextItem.getUserAccess(user));
                   }
               } catch (NoSuchElementException ee) {
                   // very likely.
               }
           }
       }
       if( currentItem == null ) {
           throw new
                   NoSuchElementException("Not found : " + aclItem);
       }
       return currentItem;
   }
   private AclItem _resolveAclItem( String aclItem )
           throws NoSuchElementException{
       AclItem result   = getAcl( aclItem ) ;
       AclItem cursor   = result ;
       String inherited;
       int i;
       for( i = 0 ;
            ( i < 200 ) &&
            ( inherited = cursor.getInheritance() ) != null ; i++ ){
          cursor = getAcl( inherited ) ;
          Enumeration<String> e = cursor.getUsers() ;
          while( e.hasMoreElements() ){
             String  user   = e.nextElement();
             result.merge( user , cursor.getUserAccess(user) ) ;
          }
       }
       if( i == 200 ) {
           throw new
                   NoSuchElementException("Infinit inheritance loop detected");
       }

       result.setInheritance(null);
       return result ;
   }
   public synchronized boolean check( String aclItem ,
                         String user ,
                         UserRelationable relations ){
       try{
          AclItem item = _fullResolveAclItem( aclItem ) ;
          return _check( item , user , relations ) ;
       }catch(Exception e){
          return false ;
       }
   }
   private boolean _check( AclItem item ,
                           String user ,
                           UserRelationable relations )
           throws NoSuchElementException{

       Boolean ok = item.getUserAccess(user) ;
       if( ok != null ) {
           return ok;
       }

       Vector<String> v = new Vector<>() ;
       Boolean x;

       v.addElement( user ) ;

       for( int i = 0 ; i < v.size() ; i++ ){
           user = v.elementAt(i);
           if( ( x = item.getUserAccess( user ) ) != null ){
              if(x) {
                  return true;
              }
              continue ;
           }
           Enumeration<String> e = relations.getParentsOf(user) ;
           while( e.hasMoreElements() ) {
               v.addElement(e.nextElement());
           }

       }
       return false ;
   }
}
