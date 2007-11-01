// $Id: GenericInfoExtractor.java,v 1.7 2006-02-03 13:23:15 patrick Exp $

package diskCacheV111.util ;

import  diskCacheV111.vehicles.* ;
import  java.util.* ;
import  java.io.* ;

import java.util.HashMap;


public class       GenericInfoExtractor 
       implements  StorageInfoExtractable {   

           
           
           
   private HashMap _extractors = new HashMap();   
           
           
   public void setStorageInfo(String pnfsMountpoint, PnfsId pnfsId, StorageInfo storageInfo, int accessMode) throws CacheException {

      StorageInfoExtractable extr = null ;

      if( storageInfo instanceof OSMStorageInfo ){
          extr = new OsmInfoExtractor() ;
      }else if( storageInfo instanceof EnstoreStorageInfo ){
          extr = new EnstoreInfoExtractor() ;
      }else{
          try{

              String hsmName = storageInfo.getClass().getName() ;
              int    postFix = hsmName.lastIndexOf("StorageInfo") ;
              if( postFix < 0 )
                  throw new
                  NumberFormatException("Wrong StorageInfo name format : "+hsmName);
                 
              String className = hsmName.substring(0,postFix) + "InfoExtractor" ;
              
              extr = (StorageInfoExtractable)Class.forName(className).newInstance();
                                   
          }catch(Exception ee ){

             throw new 
             CacheException(104,"Can't init info extractor for "+
                                storageInfo.getClass().getName()+
                                " "+ee.getMessage());

          }

      }
          
      extr.setStorageInfo( pnfsMountpoint , pnfsId , storageInfo , accessMode ) ;

    }
    
    public StorageInfo getStorageInfo(String mp, PnfsId pnfsId) throws CacheException {
       try{
          PnfsFile dir = null ;
          PnfsFile x   = PnfsFile.getFileByPnfsId( mp , pnfsId ) ;
          if( x == null )
             throw new 
             FileNotFoundCacheException( "Pnfs File not found : "+pnfsId ) ;
             
          if(  x.isDirectory() ){
             dir = x ;
          }else{
             PnfsId parent = x.getParentId() ;
             if( parent == null )
                throw new
                CacheException( 36, "Couldn't determine parent ID" ) ;

             dir = PnfsFile.getFileByPnfsId( mp , parent ) ;
          }
          
          
          String className = null ;
          String hsmType   = getHsmType( mp , dir ) ;
       
          if( ( hsmType == null ) || ( hsmType.length() < 1 ) )
             throw new
             CacheException( 37 , "Couldn't determine hsmType" ) ;

          if( hsmType.endsWith("InfoExtractor" ) ){
             //
             // be more flexible
             //
             className = hsmType ;
             //
          }else{
             // and backward compatible
             //
             // prepare for appropriate extractor class
             //
             StringBuffer sb = new StringBuffer( hsmType.toLowerCase()  ) ;
             sb.setCharAt( 0 , Character.toUpperCase( sb.charAt(0) ) ) ;
          
             className = "diskCacheV111.util."+sb.toString()+"InfoExtractor" ;
          }
          
          StorageInfo            info = null ;
          StorageInfoExtractable extr = null ;
          
          
          extr = (StorageInfoExtractable)_extractors.get(className);
          
          if( extr == null ) {
              try{
                 extr = (StorageInfoExtractable)Class.forName(className).newInstance() ;
                 _extractors.put(className, extr);
              }catch(Exception ee ){
                 throw new
                CacheException( 38 , "Can't instantiate : "+className+" "+ee ) ;
              }              
          }          
          
          info = extr.getStorageInfo( mp , pnfsId ) ;
          
          if( info instanceof GenericStorageInfo ){
            GenericStorageInfo gi = (GenericStorageInfo)info ;
            String h = getHsmName( mp , dir ) ;
            gi.setHsm( h == null ? hsmType.toLowerCase() : h.toLowerCase() ) ;
            gi.setCacheClass( getCacheClass( mp , dir ) ) ;
          }
          return info ;
          
       }catch(CacheException ce ){
          throw ce ;
       }catch(Exception e ){
          e.printStackTrace();
          throw new
          CacheException( 33 , "unexpected : "+e ) ;
       }
    
    }
    private String getCacheClass( String mp , PnfsFile dir ){
       String [] tag = dir.getTag("cacheClass") ;
       return ( tag != null ) &&
              ( tag.length > 0 ) &&
              ( tag[0] != null ) ? tag[0].trim() : null ;
       
    }
    private String getHsmName( String mp , PnfsFile dir ){
       String [] tag = dir.getTag("hsmInstance") ;
       return ( tag != null ) &&
              ( tag.length > 0 ) &&
              ( tag[0] != null ) ? tag[0].trim() : null ;
       
    }
    private String getHsmType( String mp , PnfsFile dir ){
       String [] tag = dir.getTag("hsmType") ;
       String type =
              ( tag != null ) &&
              ( tag.length > 0 ) &&
              ( tag[0] != null ) ? tag[0].trim() : "" ;
      
       if( type.length() > 0 )return type ;
       
       tag = dir.getTag("file_family") ;
       if( ( tag != null ) &&
           ( tag.length > 0 ) &&
           ( tag[0] != null ) &&
           ( tag[0].trim().length() > 0 ) )return "enstore" ;
       tag = dir.getTag("sGroup") ;
       if( ( tag != null ) &&
           ( tag.length > 0 ) &&
           ( tag[0] != null ) &&
           ( tag[0].trim().length() > 0 ) )return "osm" ;
       
       String cacheClass = getCacheClass( mp , dir ) ;
       if( ( cacheClass != null ) &&
           ( cacheClass.length() > 0 ) )return "lfs" ;
              
       return null ;
    }
    
    public static void main( String [] args ) throws Exception {
       if( args.length < 2 ){
          System.err.println( "Usage : ... <mp> <pnfsId>" ) ;
          System.exit(4);
       }
       StorageInfoExtractable sie = new GenericInfoExtractor() ;
       
       StorageInfo info = sie.getStorageInfo( args[0] , new PnfsId(args[1]) ) ;
       System.out.println( "java class        = ["+info.getClass().getName()+"]") ;
       System.out.println( "object.toString() = "+info.toString() ) ;
       System.out.println( "storage class     = "+info.getStorageClass() ) ;
       System.out.println( "cache class       = "+info.getCacheClass() ) ;
       System.out.println( "hsm               = "+info.getHsm() ) ;
       System.exit(0);
    }

}
