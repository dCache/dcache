package dmg.util ;

import java.util.* ;
import java.io.* ;
import java.lang.reflect.* ;

public class ClassLoaderFactory {

    private File              _dir      = null ;
    private ClassDataProvider _provider = null ;
    
    public ClassLoaderFactory(){}
    public ClassLoaderFactory( ClassDataProvider dataProvider ){
       _provider = dataProvider ;
    }
    public ClassLoaderFactory( String dir ){
       _dir = new File( dir ) ;
       if( ! _dir.isDirectory() ) {
           throw new IllegalArgumentException("Not a directory : " + _dir);
       }
    }
    public Class loadClass( String className ){
       
       ClassLoaderA loader = null ;
       
       if( _dir != null ) {
           loader = new ClassLoaderA(_dir);
       } else if( _provider != null ) {
           loader = new ClassLoaderA(_provider);
       } else {
           throw new
                   IllegalArgumentException("Class Load Provider not set");
       }
          
       return loader.loadClass( className ) ; 
    }
    public Class loadClass( String className , File dir ){
       ClassLoaderA loader = new ClassLoaderA( dir ) ;
       return loader.loadClass( className ) ; 
    }
    public Class loadClass( String className , ClassDataProvider dataProvider ){
       ClassLoaderA loader = new ClassLoaderA( dataProvider ) ;
       return loader.loadClass( className ) ; 
    }
    public String toString(){ 
        return _dir!=null ? _dir.toString() : "Call By Call" ; 
    }
    
} 
class ClassLoaderA extends ClassLoader {

    private File _dir = null ;
    private ClassDataProvider _provider = null ;
    
    ClassLoaderA( String dir ){
       _dir = new File( dir ) ;
       if( ! _dir.isDirectory() ) {
           throw new IllegalArgumentException("Not a directory : " + _dir);
       }
          
       
    }
    ClassLoaderA( File dir ){
       _dir = dir ;
       if( ! _dir.isDirectory() ) {
           throw new IllegalArgumentException("Not a directory : " + _dir);
       }
          
       
    }
    ClassLoaderA( ClassDataProvider dataProvider ){
       _provider = dataProvider ;
    }
    public static byte [] loadClassData( File dir , String name) {
    
       System.out.println( "loadClassData : File="+name ) ;
       File file = new File( dir , name ) ;
       try{
           long length = file.length() ;
           System.out.println( "loadClassData : length="+length ) ;
           if( length == 0 ) {
               return null;
           }
           byte [] data = new byte[(int)length] ;
           DataInputStream in = 
               new DataInputStream( new FileInputStream( file ) ) ;
           in.read( data ) ;
           in.close() ;

           return data ;
           
       }catch( Exception eee ){
           System.out.println( "loadClassData : Exception : "+eee ) ;
           return null ;
       } 
    }
    @Override
    public synchronized Class loadClass(String name ) {
        return loadClass( name , true ) ;
    }
    @Override
    public synchronized Class loadClass(String name, boolean resolve) {
        System.out.println( "Loading class "+name ) ;
        if( name.startsWith( "java" ) || 
            name.startsWith( "dmg" )     ){
            try{
               return Class.forName( name ) ;
            }catch( ClassNotFoundException cnf ){
               return null ;
            }
        }
        byte [] data = null ;
        if( _dir != null ){
           data = loadClassData( _dir , name+".class" );
           if( data == null ){
              name = name.replace( '.' , '/' ) ;
              data = loadClassData( _dir , name+".class" );
              if( data == null ) {
                  return null;
              }
           }
        }else if( _provider != null ){
           try{
              data = _provider.getClassData( name ) ;
           }catch( IOException ioe ){
              data = null ;
           }
        }else {
            data = null;
        }
           
        if( data == null ) {
            return null;
        }
        Class entry = defineClass( name , data, 0, data.length);

        if( resolve ) {
            resolveClass(entry);
        }

        return entry ;
    }


}
