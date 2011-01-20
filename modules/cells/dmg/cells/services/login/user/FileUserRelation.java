// $Id: FileUserRelation.java,v 1.1 2001-05-02 06:14:15 cvs Exp $
package dmg.cells.services.login.user  ;
import java.util.* ;
import java.io.* ;
public class FileUserRelation implements TopDownUserRelationable {

    private class ListEnumeration implements Enumeration {
        private Object [] _list ;
        private int       _position = 0 ;
        private ListEnumeration( Object [] list ){
           _list = list ;
        }
        public boolean hasMoreElements(){
           return _position < _list.length ;
        }
        public Object nextElement(){
           if( ! hasMoreElements() )
              throw new
              NoSuchElementException( "no more elments") ;
           return _list[_position++] ;
        }
    }

    private File _dbDir = null ;

    public FileUserRelation( File dbDir ){
       if( ! dbDir.isDirectory() )
          throw new
          IllegalArgumentException( "Not a directory : "+dbDir ) ;
       _dbDir = dbDir ;
    }
    public synchronized void createContainer( String container )
        throws DatabaseException {

        File c = new File( _dbDir , container ) ;
        if( c.exists() )
           throw new
           DatabaseException( 3 , "Container exists "+c ) ;
        try{
            new FileOutputStream( c ).close() ;
        }catch( IOException ee ){
           throw new
           DatabaseException( 4 , "Creation denied : "+ee.getMessage() ) ;

        }
        return ;
    }
    public void     removeContainer( String container )
        throws NoSuchElementException ,
               DatabaseException {

        File c = new File( _dbDir , container ) ;
        if( ! c.exists() )
           throw new
           NoSuchElementException( container ) ;

        Hashtable hash = loadFile( c ) ;
        if( hash.size() > 0 )
          throw new
          DatabaseException( 5 , "Not empty" ) ;

        c.delete() ;

        return ;

    }
    public Enumeration getContainers(){
       return new ListEnumeration(
            _dbDir.list(
                  new FilenameFilter(){
                        public boolean accept( File dir , String name ){
                            return ! name.startsWith(".") ;
                        }
                  }
            )
       ) ;
    }
    public synchronized Enumeration getElementsOf( String container )
        throws NoSuchElementException {


       return loadFile( new File( _dbDir , container ) ).keys() ;

    }
    public synchronized boolean isElementOf( String container , String element )
        throws NoSuchElementException {

       return loadFile( new File( _dbDir , container ) ).get(element) != null ;
    }
    public synchronized void addElement( String container , String element )
        throws NoSuchElementException {

       File c = new File( _dbDir , container ) ;
       Hashtable hash = loadFile( c ) ;
       // avoid IO
       if( hash.get( element ) != null )return ;
       hash.put( element , element ) ;
       storeFile( c , hash ) ;
       return ;
    }
    public synchronized void removeElement( String container , String element )
        throws NoSuchElementException {

       File c = new File( _dbDir , container ) ;
       Hashtable hash = loadFile( c ) ;
       // avoid IO
       if( hash.remove(element) == null )return ;
       storeFile( c , hash ) ;
       return ;
    }
    private void storeFile( File file , Hashtable hash )
            throws NoSuchElementException {
       PrintWriter pw = null ;
       File tmpFile = new File( file.getParent() , "."+file.getName() ) ;
       try{
          pw = new PrintWriter( new FileWriter( tmpFile ) ) ;
       }catch(IOException e ){
           throw new
           NoSuchElementException( "Open error on "+file ) ;
       }
       Enumeration e = hash.keys() ;
       while( e.hasMoreElements() )
          pw.println( e.nextElement().toString() ) ;
        pw.close() ;

       tmpFile.renameTo( file ) ;
       return ;
    }
    private Hashtable loadFile( File file ) throws NoSuchElementException {
        Hashtable      hash = new Hashtable() ;
        BufferedReader br   = null ;

        try{
          br = new BufferedReader( new FileReader( file ) ) ;
        }catch( IOException e ){
           throw new
           NoSuchElementException( "No found "+file ) ;
        }
        String line = null ;
        try{
           while( ( line = br.readLine() ) != null ){
              String name = line.trim() ;
              hash.put( name , name ) ;
           }
        }catch(IOException ioe ){
           throw new
           NoSuchElementException( "IOError on "+file ) ;
        }finally{
            try{ br.close() ; }catch(Exception ee){}
        }

        return hash ;
    }

    public static void main( String [] args ) throws Exception {
       if( args.length < 1 ){
         System.err.println( "Usage : ... <dbDir>" ) ;
         System.exit(4);
       }
       TopDownUserRelationable db = new FileUserRelation( new File( args[0] ) ) ;
       try{
       if( args.length > 1 ){
         if( args[1].equals( "addcontainer" ) ){
            if( args.length < 3 ){
               System.err.println( "Usage : ... <db> addcontainer <container>" ) ;
               System.exit(5) ;
            }
            db.createContainer( args[2] ) ;
         }else if( args[1].equals( "rmcontainer" ) ){
            if( args.length < 3 ){
               System.err.println( "Usage : ... <db> rmcontainer <container>" ) ;
               System.exit(5) ;
            }
            db.removeContainer( args[2] ) ;
         }else if( args[1].equals( "add" ) ){
            if( args.length < 4 ){
               System.err.println( "Usage : ... <db> add <container> <element>" ) ;
               System.exit(5) ;
            }
            db.addElement( args[2] , args[3] ) ;
         }else if( args[1].equals( "remove" ) ){
            if( args.length < 4 ){
               System.err.println( "Usage : ... <db> remove <container> <element>" ) ;
               System.exit(5) ;
            }
            db.removeElement( args[2] , args[3] ) ;
         }
       }
       }catch(Exception e){
          System.err.println( e.toString() ) ;
          System.exit(4);
       }
       Enumeration e = db.getContainers() ;
       while( e.hasMoreElements() ){
          String container = e.nextElement().toString() ;
          System.out.println( container ) ;
          Enumeration f = db.getElementsOf( container ) ;
          while( f.hasMoreElements() ){
             String element = f.nextElement().toString() ;
             System.out.println( "    "+element ) ;
          }
       }
       System.exit(0);
    }

}
