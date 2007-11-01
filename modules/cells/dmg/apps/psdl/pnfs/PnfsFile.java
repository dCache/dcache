package dmg.apps.psdl.pnfs ;

import java.util.* ;
import java.io.* ;

public class PnfsFile extends File  {

   private String  _absolute = null ;
   private PnfsId  _pnfsId   = null ;
   
   public PnfsFile( String path ){
      super( path ) ;
      _absolute = getAbsolutePath() ;
      _pnfsId   = null ;
   }
   public PnfsFile( File path , String file ){
      super( path , file ) ;
      _absolute = getAbsolutePath() ;
      _pnfsId   = null ;
   }
   private PnfsFile( String mp , String file , PnfsId id ){
      super( mp , file ) ;
      _absolute = getAbsolutePath() ;
      _pnfsId   = id ;
   }
   public boolean isPnfs(){
      if( isDirectory() ){
         File f = new File( this , ".(const)(x)" ) ;
         return f.exists() ;
      }else if( isFile() ){
         String dirString = new File( _absolute ).getParent() ;
         if( dirString == null )return false ;
         File f = new File( dirString , ".(const)(x)" ) ;
         return f.exists() ;     
      }else return false ;
   }
   public boolean delete(){
     if( _pnfsId == null )return super.delete() ; 
     return false ;
   }
   public boolean setLength( long length ){
      if( _pnfsId == null ){
         if( ! isFile() )return false ;
         String dirString = new File( _absolute ).getParent() ;
         if( dirString == null )return false  ;
         File f = 
         new File( dirString , 
                   ".(fset)("+getName()+")(size)("+length+")" ) ;

         if( length == 0L ){
            return f.length() == 0L ? true :  f.delete() ;
         }else{
            if( f.length() != 0L )return false ;
            try{
                FileOutputStream out = new FileOutputStream( f ) ;
                out.close() ;
                long l = length() ;
                return length() == length ;
            }catch( IOException e ){
                return false ;
            }
         }
      }else{
         String parent = getParent() ;
         File f = new File( parent , ".(pset)("+_pnfsId+")(size)("+length+")" ) ;
         if( length == 0 ){
            f.delete() ; // failes if already zero
            return true ;
         }else{
            try{
                FileOutputStream out = new FileOutputStream( f ) ;
                out.close() ;
                return f.length() == length ;
            }catch( IOException ioe ){
                return false ;
            }
         }
      }
   }
   public File getLevelFile( int level ){
      if( ( level<=0) && ( level > 7 ) )
        throw new IllegalArgumentException( "Illegal Level "+level ) ;
      
      if( _pnfsId != null ){
         return new File( _absolute+"("+level+")" ) ;
      }else{   
         String dirString = new File( _absolute ).getParent() ;
         if( dirString == null )return null ;
         return new File( dirString , ".(use)("+level+")("+getName()+")" ) ;
      }
   }
   public PnfsId getPnfsId(){
      String dirString = new File( _absolute ).getParent() ;
      if( dirString == null )return null ;
      File f = new File( dirString , ".(id)("+getName()+")" ) ;
      if( ! f.exists() )return null ;
      try{
         BufferedReader r = new BufferedReader(
                               new FileReader( f ) ) ;
         String idString = r.readLine() ;
         r.close() ;
         if( idString == null )return null ;
         return new PnfsId( idString ) ;
      }catch( Exception e ){
         e.printStackTrace() ;
         return null ;
      } 
   
   }
   public String [] getTags(){
      if( ( ! isDirectory() ) || ( ! isPnfs() ) )return null ;
      File f = new File( this , ".(tags)(x)" ) ;
      if( ! f.exists() )return null ;
      Vector v = new Vector() ;
      try{
         String line = null ;
         BufferedReader r = new BufferedReader(
                               new FileReader( f ) ) ;
         while( ( line = r.readLine() ) != null ){
            v.addElement( line ) ;
         }
         r.close() ;
      }catch( IOException ee ){
         return null ;
      }
      String [] a = new String[v.size()] ;
      String tagName = null ;
      int j = 0 ;
      for( int i= 0 ; i < v.size() ; i ++ ){
         if( ( a[j] = tagNameOf( (String)v.elementAt(i) ) ) == null )
           continue ;
         j++ ;
      }
      if( j < v.size() ){
          String [] b = new String[j] ;
          System.arraycopy( a , 0 , b , 0 , j ) ;
          return b ;
      }
      return a ;
   }
   private String tagNameOf( String str ){
      if( ( ! str.startsWith( ".(tag)(" ) ) ||
          ( ! str.endsWith( ")" )         )    )return null ;
      return str.substring( 7 , str.length()-1 ) ;
   }
   public String [] getTag( String tagName ){
      if( ( ! isDirectory() ) || ( ! isPnfs() ) )return null ;
      File f = new File( this , ".(tag)("+tagName+")" ) ;
      if( ! f.exists() )return null ;
      try{
         Vector v = new Vector() ;
         String line = null ;
         BufferedReader r = new BufferedReader(
                               new FileReader( f ) ) ;
         while( ( line = r.readLine() ) != null ){
            v.addElement( line ) ;
         }
         r.close() ;
         String [] a = new String[v.size()] ;
         v.copyInto( a ) ;
         return a ;
      }catch( IOException ee ){
         return null ;
      }
   }
   /*
   public String getAbsolutePath(){
      System.out.println( "Absolute path required" ) ;
      return super.getAbsolutePath() ;
   }
   public String getCanonicalPath() throws IOException {
      System.out.println( "Canonical path required" ) ;
      return super.getCanonicalPath() ;
   }
   */
   public static PnfsFile getFileByPnfsId( String mountpoint , PnfsId id ){
       PnfsFile mp = new PnfsFile( mountpoint ) ;
       if( ( ! mp.isDirectory() ) || ( ! mp.isPnfs() ) )return null ;
       PnfsFile f = new PnfsFile( mountpoint , ".(access)("+id+")" , id ) ;
       if( ! f.exists() )return null ;
       return f ;
   }
   public static void main( String [] args ){
      if( args.length < 2 ){
         System.out.println( "Usage : ... <command> <path> [arguments]" ) ;
         System.out.println( "Usage : ... ispnfs <path>" ) ;
         System.out.println( "Usage : ... id <path>" ) ;
         System.out.println( "Usage : ... gettags <dirPath>" ) ;
         System.out.println( "Usage : ... gettag <dirPath> <tagname>" ) ;
         System.out.println( "Usage : ... readlevel <dirPath> <level>" ) ;
         System.out.println( "Usage : ... readxlevel <mountpoint> <pnfsId> <level>" ) ;
         System.out.println( "Usage : ... setsize <filePath> <size>" ) ;
         System.out.println( "Usage : ... psetsize <mountpoint>  <pnfsId> <size>" ) ;
         System.exit(4) ;
      }
      try{
         String command = args[0] ;
         String path    = args[1] ;
         PnfsFile f = new PnfsFile( path ) ;
         if( command.equals( "ispnfs" ) ){
             System.out.println( path+" : "+f.isPnfs() ) ;
         }else if( command.equals( "id" ) ){
            PnfsId id = f.getPnfsId() ;
            if( id == null ){
              System.out.println( "Can't determine pnfs id" ) ;
            }else{            
              System.out.println( path+" : "+f.getPnfsId().toString() ) ;
            }
         }else if( command.equals( "gettag" ) ){
            if( args.length < 3 ){
               System.err.println( "Not enought arguments" ) ;
               System.exit(45);
            }
            if( ! f.isDirectory() ){
               System.err.println( "Not a directory : "+path ) ;
               System.exit(45);
            }
            String [] tag = f.getTag( args[2] ) ;
            if( tag == null ){
              System.out.println( "Tag "+args[2]+" not found" ) ;
              System.exit(4);
            }else{
              for( int j = 0 ; j < tag.length ; j++ ){
                 System.out.println( args[1]+" : "+tag[j] ) ;
              }
            }
            
         }else if( command.equals( "gettags" ) ){
            String [] tags = f.getTags() ;
            if( tags == null ){
              System.out.println( "No tags found" ) ;
              System.exit(4);
            }else{
              for( int j = 0 ; j < tags.length ; j++ ){
                 System.out.println( tags[j] ) ;
              }
            }
         }else if( command.equals( "readlevel" ) ){
            if( args.length < 3 ){
               System.err.println( "Not enought arguments" ) ;
               System.exit(45);
            }
            int level = new Integer( args[2] ).intValue() ;
            File lf  = f.getLevelFile( level ) ;
            if( lf == null ){
              System.out.println( "Can't get levelfile" ) ;
              System.exit(4);
            }
            try{
               BufferedReader r = new BufferedReader(
                                   new FileReader( lf ) ) ;
               String line = null ;
               while( ( line = r.readLine() ) != null ){
                  System.out.println( line ) ;
               }
               r.close() ;
            }catch( IOException e ){
               System.err.println( "Exception : "+e ) ;
            }
         }else if( command.equals( "readxlevel" ) ){
            if( args.length < 4 ){
               System.err.println( "Not enought arguments" ) ;
               System.exit(45);
            }
            PnfsId id = new PnfsId( args[2] ) ;
            int level = new Integer( args[3] ).intValue() ;
            
            
            PnfsFile x  = PnfsFile.getFileByPnfsId( f.getAbsolutePath() , id ) ;
            if( x == null ){
              System.out.println( "Can't get PnfsFile.getFileByPnfsId" ) ;
              System.exit(4);
            }
            File lf  = x.getLevelFile( level ) ;
            try{
               BufferedReader r = new BufferedReader(
                                   new FileReader( lf ) ) ;
               String line = null ;
               while( ( line = r.readLine() ) != null ){
                  System.out.println( line ) ;
               }
               r.close() ;
            }catch( IOException e ){
               System.err.println( "Exception : "+e ) ;
            }
         }else if( command.equals( "test" ) ){
            try{
               BufferedReader r = new BufferedReader(
                                   new FileReader( f ) ) ;
               r.close() ;
            }catch( IOException e ){
               System.err.println( "Exception : "+e ) ;
            }
         }else if( command.equals( "psetsize" ) ){
            if( args.length < 4 ){
               System.err.println( "Not enought arguments" ) ;
               System.exit(45);
            }
            PnfsId id   = new PnfsId( args[2] ) ;
                      
            PnfsFile x  = PnfsFile.getFileByPnfsId( f.getAbsolutePath() , id ) ;
            if( x == null ){
              System.out.println( "Can't get PnfsFile.getFileByPnfsId" ) ;
              System.exit(4);
            }
            int size = new Integer( args[3] ).intValue() ;
            boolean y= x.setLength( (long)size ) ;
            System.out.println( "Result : "+y ) ;
          }else if( command.equals( "setsize" ) ){
            if( args.length < 3 ){
               System.err.println( "Not enought arguments" ) ;
               System.exit(45);
            }
            int size = new Integer( args[2] ).intValue() ;
            boolean x = f.setLength( (long)size ) ;
            System.out.println( "Result : "+x ) ;
         }
      }catch( Exception e ){
         e.printStackTrace() ;
         System.exit(4) ;
      }
   }
 
}
