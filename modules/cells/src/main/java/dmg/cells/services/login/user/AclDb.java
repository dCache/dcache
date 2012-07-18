// $Id: AclDb.java,v 1.4 2006-12-15 10:58:14 tigran Exp $
package dmg.cells.services.login.user  ;
import java.io.* ;
import java.util.* ;

public class AclDb {

   private class AclItem implements AcDictionary {
       private String    _name     = null ;
       private Hashtable _users    = new Hashtable() ;
       private String    _inherits = null ;
       private AclItem( String name ){ _name = name ; }
       private void setInheritance( String aclItem ){
           _inherits = aclItem ;
       }
       private void addAccess( String user , boolean access ){
           _users.put( user , access) ;
       }
       private Enumeration getUsers(){ return _users.keys() ; }
       private void removeUser(String user ){
          _users.remove( user ) ;
       }
       private Boolean getUserAccess(String user)
               throws NoSuchElementException{
           return  (Boolean)_users.get(user) ;
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
       public Enumeration getPrincipals(){
           return _users.keys() ;
       }
       @Override
       public boolean getPermission( String principal ){
           Boolean ok = (Boolean)_users.get(principal) ;
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
           AclItem item   = new AclItem(_name) ;
           item._users    = (Hashtable)_users.clone() ;
           item._inherits = _inherits ;
           return item ;
       }
   }
   private File      _aclDir = null ;
   private AgingHash _hash   = new AgingHash(20) ;
   public AclDb( File aclDir ){
      if( ! aclDir.isDirectory() ) {
          throw new
                  IllegalArgumentException("Not a acl DB(not a dir)" + "aclDir \"" + aclDir
                  .getPath() + " " + aclDir.getName() + "\"");
      }
      _aclDir = aclDir ;
   }
   private void putAcl( String aclName , AclItem item )
           throws DatabaseException{
       _storeAcl( aclName , item ) ;
       _hash.put( aclName , item ) ;
       return ;
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

       File file = new File( _aclDir , "."+aclName ) ;
       PrintWriter pw = null ;
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
      Enumeration e = item.getUsers() ;
      while( e.hasMoreElements() ){
          String user = e.nextElement().toString() ;
          Boolean access = item.getUserAccess(user) ;
          pw.println(user+"="+(access ?"allowed":"denied")) ;
      }
      pw.close() ;
      file.renameTo( new File( _aclDir , aclName ) ) ;
      return ;
   }
   private AclItem _loadAcl( String aclName )
           throws NoSuchElementException {

       File file = new File( _aclDir , aclName ) ;
       if( ! file.exists() ) {
           throw new
                   NoSuchElementException("Acl  not found : " + aclName);
       }

        BufferedReader br   = null ;

        try{
          br = new BufferedReader( new FileReader( file ) ) ;
        }catch( IOException e ){
           throw new
           NoSuchElementException( "Not found "+file ) ;
        }
        String line = null ;
        StringTokenizer st   = null ;
        AclItem         item = new AclItem( aclName ) ;
        String user = null , access = null ;
        try{
           while( ( line = br.readLine() ) != null ){
              st     = new StringTokenizer( line , "=" ) ;
              user   = st.nextToken() ;
              access = st.nextToken() ;
              if( user.equals("$") ){
                 item.setInheritance(access) ;
              }else if( access.equals("allowed" ) ){
                 item.addAccess( user , true ) ;
              }else if( access.equals("denied" ) ){
                 item.addAccess( user , false ) ;
              }
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
   public synchronized void createAclItem( String aclItem )
          throws DatabaseException {
       putAcl( aclItem , new AclItem(aclItem) ) ;
   }
   public synchronized void removeAclItem( String aclItem )
   {
      _hash.remove( aclItem ) ;
      new File( _aclDir , aclItem ).delete() ;
      return  ;
   }
   public synchronized void setInheritance( String aclItem , String inheritsFrom)
          throws DatabaseException {
       AclItem item = getAcl( aclItem ) ;
       item.setInheritance( inheritsFrom ) ;
       putAcl( aclItem , item ) ;
       return ;
   }
   public synchronized void addAllowed( String aclItem , String user )
          throws DatabaseException {
       AclItem item = getAcl( aclItem ) ;
       item.addAccess( user , true ) ;
       putAcl( aclItem , item ) ;
       return ;
   }
   public synchronized void addDenied( String aclItem , String user )
          throws DatabaseException {

       AclItem item = getAcl( aclItem ) ;
       item.addAccess( user , false ) ;
       putAcl( aclItem , item ) ;
       return ;
   }
   public synchronized void removeUser( String aclItem , String user )
          throws DatabaseException {

       AclItem item = getAcl( aclItem ) ;
       item.removeUser( user ) ;
       putAcl( aclItem , item ) ;
       return ;
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
       String [] array = null ;
       if( f < 0 ){
         array = new String[2] ;
         array[0] = aclItem ;
         array[1] = "*.*.*" ;
       }else if( f == l ){
         String a = aclItem.substring(0,f) ;
         String e = aclItem.substring(l+1) ;
         array = new String[4] ;
         array[0] = aclItem ;
         array[1] = a+"."+e+".*" ;
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
                   sb.append(mm[j]).append(".");
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
            for( int i = 0 ; i < mask.length ; i++ ){
                array[k++] = className+"."+mask[i]+"*" ;
            }
            array[k++] = className+".*.*" ;
         }else if( instanceName.equals("*") ){
            array = new String[3] ;
            array[k++] = className+".*."+actionName ;
            array[k++] = className+".*.*" ;
         }else{
            array = new String[2+2*mask.length] ;
            for( int i = 0 ; i < mask.length ; i++ ){
                array[k++] = className+"."+mask[i]+actionName ;
                array[k++] = className+"."+mask[i]+"*" ;
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
       for( int i = 0 ; i < array.length ; i++ ){
          if( currentItem == null ){
             try{
                currentItem = _resolveAclItem(array[i]) ;
             }catch(NoSuchElementException ee){
               // no problem;
             }
          }else{
             try{
                AclItem nextItem = _resolveAclItem( array[i] ) ;
                Enumeration e = nextItem.getUsers() ;
                while( e.hasMoreElements() ){
                   String  user   = (String)e.nextElement() ;
                   currentItem.merge( user , nextItem.getUserAccess(user) ) ;
                }
             }catch( NoSuchElementException ee ){
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
       String inherited = null ;
       int i = 0;
       for( i = 0 ;
            ( i < 200 ) &&
            ( inherited = cursor.getInheritance() ) != null ; i++ ){
          cursor = getAcl( inherited ) ;
          Enumeration e = cursor.getUsers() ;
          while( e.hasMoreElements() ){
             String  user   = (String)e.nextElement() ;
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

       Vector  v = new Vector() ;
       Boolean x = null ;

       v.addElement( user ) ;

       for( int i = 0 ; i < v.size() ; i++ ){
           user = (String)v.elementAt(i) ;
           if( ( x = item.getUserAccess( user ) ) != null ){
              if(x) {
                  return true;
              }
              continue ;
           }
           Enumeration e = relations.getParentsOf( user ) ;
           while( e.hasMoreElements() ) {
               v.addElement(e.nextElement());
           }

       }
       return false ;
   }
}
