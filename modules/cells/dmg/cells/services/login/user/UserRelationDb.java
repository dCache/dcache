// $Id: UserRelationDb.java,v 1.1.2.1 2006-12-06 09:51:57 tigran Exp $
package dmg.cells.services.login.user  ;
import java.util.* ;
import java.io.* ;

public class UserRelationDb {
 
    private class DEnumeration implements Enumeration {
        public boolean hasMoreElements(){ return false ; }
        public Object nextElement(){ return null ;}
    }
    private class ElementItem {
       private Hashtable _parents = null ;
       private Hashtable _childs  = null ;
       private void addParent(String parent){
          if( _parents == null )_parents = new Hashtable() ;
          _parents.put(parent,parent) ;
       }
       private void addChild( String child ){
          if( _childs == null )_childs = new Hashtable() ;
          _childs.put(child,child) ;
       }
       private Enumeration parents(){ 
           return _parents == null ? new DEnumeration() : _parents.keys() ;
       }
       private Enumeration children(){ 
           return _childs == null ? new DEnumeration() : _childs.keys() ;
       }
    }
    private File      _dbDir    = null ;
    private Hashtable _elements = null ;
    public UserRelationDb( File dbDir )throws DatabaseException {
        if( ( ! dbDir.exists() ) || ( ! dbDir.isDirectory() ) )
          throw new
          DatabaseException( 11 , "Not a directory : "+dbDir ) ;
          
        _dbDir = dbDir ;
        
        _loadElements() ;
    }
    public static Hashtable loadAcl( File aclFile ){
       Hashtable      acl = new Hashtable() ;
       BufferedReader br  = null ;
       try{
          br = new BufferedReader(
                    new FileReader( aclFile ) ) ;
          String line  = null ;
          StringTokenizer st = null ;
          String name  = null ;
          String value = null ;
          while( ( line = br.readLine() ) != null ){
             st = new StringTokenizer(line,"=") ;
             try{
                name  = st.nextToken() ;
                value = st.nextToken() ;
                acl.put( name , 
                		Boolean.valueOf( value.equals("allowed") )
                       ) ;
             }catch(Exception ee){
                continue ;
             }
          }
       }catch( IOException ie ){
       }finally{
          try{ br.close() ; }catch(Exception ee){}
       }
       return acl ;
    }
    public void display(){
       Enumeration all = _elements.keys() ;
       while( all.hasMoreElements() ){
           String name = (String)all.nextElement() ;
           ElementItem item = (ElementItem)_elements.get(name) ;
           System.out.println(name);
           Enumeration e = null ;
           e = item.parents() ;
           while( e.hasMoreElements() ){
              System.out.println("   p:"+e.nextElement());
           }
           e = item.children() ;
           while( e.hasMoreElements() ){
              System.out.println("   c:"+e.nextElement());
           }
       }
    }
    private void _loadElements() throws DatabaseException{
        String [] elements = _dbDir.list( 
                     new FilenameFilter(){        
                        public boolean accept( File dir , String name ){
                           return ! name.startsWith(".") ;
                        }
                     } ) ;
        Hashtable hash = new Hashtable() ;
        for( int i = 0 ; i < elements.length ; i++ ){
            File file = new File( _dbDir , elements[i] ) ;
            BufferedReader br = null ;
            ElementItem item  = null , x = null;
            if( ( item = (ElementItem)hash.get( elements[i] ) ) == null ){
                hash.put( elements[i] , item = new ElementItem() ) ;
            }
            try{
               br = new BufferedReader(
                         new FileReader( file ) ) ;
               String line = null ;
               while( ( line = br.readLine() ) != null ){
                   String name = line.trim() ;
                   if( name.length() == 0 )continue ;
                   if( name.charAt(0) == '#' )continue ;
                   item.addChild(name) ;
                   if( ( x = (ElementItem)hash.get(name) ) == null ){
                       hash.put(name , x = new ElementItem() ) ;
                   }
                   x.addParent( elements[i] ) ;
               }
            }catch( IOException ie ){
            }finally{
               try{ br.close() ; }catch(Exception ee){}
            }
        }
        _elements = hash ;
    }
    public boolean check( String user , Hashtable acl ){
        Boolean ok = null ;
        if( ( ok = (Boolean)acl.get(user) ) != null )return ok.booleanValue() ;
        
        Vector v = new Vector() ;
        String p = null ;
        ElementItem item = null ;
        Boolean     x    = null ;
        v.addElement( user ) ;
        
        for( int i = 0 ; i < v.size() ; i++ ){
            p = (String)v.elementAt(i) ;
            if( ( x = (Boolean)acl.get(p) ) != null ){
               if( x.booleanValue() )return true ;
               continue ;
            }
            if( (item = (ElementItem)_elements.get(p)) != null ){
               Enumeration e = item.parents() ;
               while( e.hasMoreElements() )
                  v.addElement(e.nextElement()) ;
            } 
        }
        return false ;
    }
    public static void main( String [] args ) throws Exception {
       if( args.length < 1 ){
          System.err.println( "Usage : ... <dbDirectory> [<acl> <user>]");
          System.exit(4);
       }
       File   dbDir   = new File(args[0]);
       
       UserRelationDb db = new UserRelationDb( dbDir ) ;
       if( args.length < 3 ){
          db.display();
       }else{
          File   aclFile = new File( args[1] ) ;
          String user    = args[2] ;
          Hashtable acl = db.loadAcl( aclFile ) ;
          boolean rc = db.check( user , acl ) ;
          System.out.println( "user="+user+";acl="+aclFile+";allowed="+rc) ;
       }
    }
}

