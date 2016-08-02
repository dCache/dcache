// $Id: UserRelationDb.java,v 1.2 2006-12-15 10:58:14 tigran Exp $
package dmg.cells.services.login.user  ;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.Vector;

public class UserRelationDb {

    private static class DEnumeration<T> implements Enumeration<T> {
        @Override
        public boolean hasMoreElements(){ return false ; }
        @Override
        public T nextElement(){ return null ;}
    }
    private static class ElementItem {
       private Hashtable<String, String> _parents;
       private Hashtable<String, String> _childs;
       private void addParent(String parent){
          if( _parents == null ) {
              _parents = new Hashtable<>();
          }
          _parents.put(parent,parent) ;
       }
       private void addChild( String child ){
          if( _childs == null ) {
              _childs = new Hashtable<>();
          }
          _childs.put(child,child) ;
       }
       private Enumeration<String> parents(){
           return _parents == null ? new DEnumeration<>() : _parents.keys() ;
       }
       private Enumeration<String> children(){
           return _childs == null ? new DEnumeration<>() : _childs.keys() ;
       }
    }
    private File      _dbDir;
    private Hashtable<String, ElementItem> _elements;
    public UserRelationDb( File dbDir )throws DatabaseException {
        if( ( ! dbDir.exists() ) || ( ! dbDir.isDirectory() ) ) {
            throw new
                    DatabaseException(11, "Not a directory : " + dbDir);
        }

        _dbDir = dbDir ;

        _loadElements() ;
    }
    public static  Map<String, Boolean> loadAcl( File aclFile ){
       Map<String, Boolean>      acl = new Hashtable<>() ;
       BufferedReader br  = null ;
       try{
          br = new BufferedReader(
                    new FileReader( aclFile ) ) ;
          String line;
          StringTokenizer st;
          String name;
          String value;
          while( ( line = br.readLine() ) != null ){
             st = new StringTokenizer(line,"=") ;
             try{
                name  = st.nextToken() ;
                value = st.nextToken() ;
                acl.put( name ,
                        value.equals("allowed")
                       ) ;
             }catch(Exception ee){
             }
          }
       }catch( IOException ie ){
       }finally{
          if( br != null ) { try{ br.close() ; }catch(IOException ee){} }
       }
       return acl ;
    }
    public void display(){
        for (Map.Entry<String, ElementItem> entry : _elements.entrySet()) {
            ElementItem item = entry.getValue();
            System.out.println(entry.getKey());
            Enumeration<String> e = item.parents();
            while (e.hasMoreElements()) {
                System.out.println("   p:" + e.nextElement());
            }
            e = item.children();
            while (e.hasMoreElements()) {
                System.out.println("   c:" + e.nextElement());
            }
        }
    }
    private void _loadElements()
    {
        String [] elements = _dbDir.list((dir, name) -> ! name.startsWith("."));
        Hashtable<String,ElementItem > hash = new Hashtable<>() ;
        for (String element : elements) {
            File file = new File(_dbDir, element);
            BufferedReader br = null;
            ElementItem item, x;
            if ((item = hash.get(element)) == null) {
                hash.put(element, item = new ElementItem());
            }
            try {
                br = new BufferedReader(
                        new FileReader(file));
                String line;
                while ((line = br.readLine()) != null) {
                    String name = line.trim();
                    if (name.isEmpty()) {
                        continue;
                    }
                    if (name.charAt(0) == '#') {
                        continue;
                    }
                    item.addChild(name);
                    if ((x = hash.get(name)) == null) {
                        hash.put(name, x = new ElementItem());
                    }
                    x.addParent(element);
                }
            } catch (IOException ie) {
            } finally {
                if (br != null) {
                    try {
                        br.close();
                    } catch (IOException ee) {
                    }
                }
            }
        }
        _elements = hash ;
    }
    public boolean check( String user ,  Map<String, Boolean> acl ){
        Boolean ok;
        if( ( ok = acl.get(user) ) != null ) {
            return ok;
        }

        Vector<String> v = new Vector<>() ;
        String p;
        ElementItem item;
        Boolean     x;
        v.addElement( user ) ;

        for( int i = 0 ; i < v.size() ; i++ ){
            p = v.elementAt(i) ;
            if( ( x = acl.get(p) ) != null ){
               if(x) {
                   return true;
               }
               continue ;
            }
            if( (item = _elements.get(p)) != null ){
               Enumeration<String> e = item.parents() ;
               while( e.hasMoreElements() ) {
                   v.addElement(e.nextElement());
               }
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
          Map<String, Boolean> acl = UserRelationDb.loadAcl( aclFile ) ;
          boolean rc = db.check( user , acl ) ;
          System.out.println( "user="+user+";acl="+aclFile+";allowed="+rc) ;
       }
    }
}

