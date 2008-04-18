// $Id$
package diskCacheV111.util ;

/**
  *
  *
  * @author Patrick Fuhrmann
  * @version 0.2, 19 Nov 2005
  */


public class Version {
    private static String __specVersion = "0.0.0" ;
    private static String __specTitle   = "Unknown" ;
    private static String __specVendor  = "Unknown" ;
    static {
       try{
           Class c = diskCacheV111.util.Version.class ;
           Package p = Package.getPackage("diskCacheV111.util");
           if( p != null ){
               String tmp = null ;
               p.getSpecificationTitle() ;
               if( ( tmp =  p.getSpecificationTitle() ) != null   )__specTitle   = tmp ;
               if( ( tmp =  p.getSpecificationVersion() ) != null )__specVersion = tmp ;
               if( ( tmp =  p.getSpecificationVendor() ) != null  )__specVendor  = tmp ;
           }
       }catch(Exception ee){}
    
    }
    public static String getVersion(){ return __specVersion ; }
    public static void main( String [] args ){
       try{
           Class c = Class.forName( "dmg.util.Args" ) ;
       }catch(Exception iee){
           System.err.println("Load Error : Cells not found") ;
           System.exit(4);
       }
       if( args.length > 0 ){
                  System.out.println("SpecificationTitle:   "+__specTitle);
                  System.out.println("SpecificationVersion: "+__specVersion);
                  System.out.println("SpecificationVendor:  "+__specVendor);
       }else{
          System.out.println(__specVersion) ;
       }
       System.exit(0);
    }
}
