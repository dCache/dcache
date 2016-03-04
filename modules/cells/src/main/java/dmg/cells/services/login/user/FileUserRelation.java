// $Id: FileUserRelation.java,v 1.1 2001-05-02 06:14:15 cvs Exp $
package dmg.cells.services.login.user  ;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Map;
import java.util.NoSuchElementException;
public class FileUserRelation implements TopDownUserRelationable {

    private class ListEnumeration implements Enumeration<String> {
        private String [] _list ;
        private int       _position;
        private ListEnumeration( String [] list ){
           _list = list ;
        }
        @Override
        public boolean hasMoreElements(){
           return _position < _list.length ;
        }
        @Override
        public String nextElement(){
           if( ! hasMoreElements() ) {
               throw new
                       NoSuchElementException("no more elments");
           }
           return _list[_position++] ;
        }
    }

    private File _dbDir;

    public FileUserRelation( File dbDir ){
       if( ! dbDir.isDirectory() ) {
           throw new
                   IllegalArgumentException("Not a directory : " + dbDir);
       }
       _dbDir = dbDir ;
    }
    @Override
    public synchronized void createContainer( String container )
        throws DatabaseException {

        File c = new File( _dbDir , container ) ;
        if( c.exists() ) {
            throw new
                    DatabaseException(3, "Container exists " + c);
        }
        try{
            new FileOutputStream( c ).close() ;
        }catch( IOException ee ){
           throw new
           DatabaseException( 4 , "Creation denied : "+ee.getMessage() ) ;

        }
    }
    @Override
    public void     removeContainer( String container )
        throws NoSuchElementException ,
               DatabaseException {

        File c = new File( _dbDir , container ) ;
        if( ! c.exists() ) {
            throw new
                    NoSuchElementException(container);
        }

        Hashtable<String, String> hash = loadFile(c) ;
        if( hash.size() > 0 ) {
            throw new
                    DatabaseException(5, "Not empty");
        }

        c.delete() ;

    }
    @Override
    public Enumeration<String> getContainers(){
       return new ListEnumeration(_dbDir.list((dir, name) -> ! name.startsWith(".")));
    }
    @Override
    public synchronized Enumeration<String> getElementsOf( String container )
        throws NoSuchElementException {


       return loadFile( new File( _dbDir , container ) ).keys() ;

    }
    @Override
    public synchronized boolean isElementOf( String container , String element )
        throws NoSuchElementException {

       return loadFile( new File( _dbDir , container ) ).get(element) != null ;
    }
    @Override
    public synchronized void addElement( String container , String element )
        throws NoSuchElementException {

       File c = new File( _dbDir , container ) ;
       Hashtable<String, String> hash = loadFile(c) ;
       // avoid IO
       if( hash.get( element ) != null ) {
           return;
       }
       hash.put( element , element ) ;
       storeFile( c , hash ) ;
    }
    @Override
    public synchronized void removeElement( String container , String element )
        throws NoSuchElementException {

       File c = new File( _dbDir , container ) ;
       Hashtable<String, String> hash = loadFile(c) ;
       // avoid IO
       if( hash.remove(element) == null ) {
           return;
       }
       storeFile( c , hash ) ;
    }
    private void storeFile( File file , Map<String, String> hash )
            throws NoSuchElementException {
       PrintWriter pw;
       File tmpFile = new File( file.getParent() , "."+file.getName() ) ;
       try{
          pw = new PrintWriter( new FileWriter( tmpFile ) ) ;
       }catch(IOException e ){
           throw new
           NoSuchElementException( "Open error on "+file ) ;
       }
        for (Object o : hash.keySet()) {
            pw.println(o.toString());
        }
        pw.close() ;

       tmpFile.renameTo( file ) ;
    }
    private Hashtable<String, String> loadFile(File file) throws NoSuchElementException {
        Hashtable<String, String> hash = new Hashtable<>() ;
        BufferedReader br;

        try{
          br = new BufferedReader( new FileReader( file ) ) ;
        }catch( IOException e ){
           throw new
           NoSuchElementException( "No found "+file ) ;
        }
        String line;
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

    public static void main( String [] args )
    {
       if( args.length < 1 ){
         System.err.println( "Usage : ... <dbDir>" ) ;
         System.exit(4);
       }
       TopDownUserRelationable db = new FileUserRelation( new File( args[0] ) ) ;
       try{
       if( args.length > 1 ){
           switch (args[1]) {
           case "addcontainer":
               if (args.length < 3) {
                   System.err
                           .println("Usage : ... <db> addcontainer <container>");
                   System.exit(5);
               }
               db.createContainer(args[2]);
               break;
           case "rmcontainer":
               if (args.length < 3) {
                   System.err
                           .println("Usage : ... <db> rmcontainer <container>");
                   System.exit(5);
               }
               db.removeContainer(args[2]);
               break;
           case "add":
               if (args.length < 4) {
                   System.err
                           .println("Usage : ... <db> add <container> <element>");
                   System.exit(5);
               }
               db.addElement(args[2], args[3]);
               break;
           case "remove":
               if (args.length < 4) {
                   System.err
                           .println("Usage : ... <db> remove <container> <element>");
                   System.exit(5);
               }
               db.removeElement(args[2], args[3]);
               break;
           }
       }
       }catch(Exception e){
          System.err.println( e.toString() ) ;
          System.exit(4);
       }
       Enumeration<String> e = db.getContainers() ;
       while( e.hasMoreElements() ){
          String container = e.nextElement();
          System.out.println( container ) ;
          Enumeration<String> f = db.getElementsOf(container) ;
          while( f.hasMoreElements() ){
             String element = f.nextElement();
             System.out.println( "    "+element ) ;
          }
       }
       System.exit(0);
    }

}
