// $Id: GenericInfoExtractor.java,v 1.7 2006-02-03 13:23:15 patrick Exp $

package diskCacheV111.util ;

import  diskCacheV111.vehicles.* ;
import dmg.util.Args;

import  java.util.* ;
import  java.io.* ;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

import java.util.HashMap;
import diskCacheV111.util.AccessLatency;
import diskCacheV111.util.RetentionPolicy;

public class       GenericInfoExtractor
       implements  StorageInfoExtractable
{
    private final static String SUFFIX = "StorageInfo";

    private Map<String, StorageInfoExtractable> _extractors = new HashMap();

    /**
     * Returns an info extractor for the given HSM.
     */
    private synchronized StorageInfoExtractable getExtractor(String hsm)
        throws CacheException
    {
        if ((hsm == null) || (hsm.length() == 0) || hsm.equalsIgnoreCase("generic"))
            throw new IllegalArgumentException("Invalid HSM type");

        StorageInfoExtractable extr = _extractors.get(hsm);
        if (extr == null) {
            String className;

            if (hsm.endsWith("InfoExtractor")) {
                /* be more flexible
                 */
                className = hsm;
            } else {
                /* be backward compatible
                 */
                StringBuffer sb = new StringBuffer(hsm.toLowerCase());
                sb.setCharAt(0, Character.toUpperCase(sb.charAt(0)));
                className = "diskCacheV111.util." + sb + "InfoExtractor";
            }
            try {
                Constructor constructor = Class.forName(className)
                    .getConstructor();
                extr = (StorageInfoExtractable)
                    constructor.newInstance();
                _extractors.put(hsm, extr);
            } catch (ClassNotFoundException e) {
                throw new CacheException(CacheException.UNEXPECTED_SYSTEM_EXCEPTION,
                                         className + " not found");
            } catch (NoSuchMethodException e) {
                throw new CacheException(CacheException.UNEXPECTED_SYSTEM_EXCEPTION,
                                         "Cannot instantiate " + className + ": " + e.getMessage());
            } catch (InvocationTargetException e) {
                throw new CacheException(CacheException.UNEXPECTED_SYSTEM_EXCEPTION,
                                         "Cannot instantiate " + className + ": " + e.getMessage());
            } catch (IllegalAccessException e) {
                throw new CacheException(CacheException.UNEXPECTED_SYSTEM_EXCEPTION,
                                         "Cannot instantiate " + className + ": " + e.getMessage());
            } catch (InstantiationException e) {
                throw new CacheException(CacheException.UNEXPECTED_SYSTEM_EXCEPTION,
                                         "Cannot instantiate " + className + ": " + e.getMessage());
            }
        }
        return extr;
    }

    public void setStorageInfo(String pnfsMountpoint, PnfsId pnfsId,
                               StorageInfo storageInfo, int accessMode)
        throws CacheException
    {

        StorageInfoExtractable extr;

        if (storageInfo instanceof OSMStorageInfo) {
            extr = new OsmInfoExtractor();
        } else if (storageInfo instanceof EnstoreStorageInfo) {
            extr = new EnstoreInfoExtractor();
        } else {
            String hsmName = storageInfo.getClass().getSimpleName();
            if (!hsmName.endsWith(SUFFIX)) {
                throw new IllegalArgumentException("Wrong StorageInfo name format : " + hsmName);
            }
            hsmName = hsmName.substring(0, hsmName.length() - SUFFIX.length());
            extr = getExtractor(hsmName);
        }

        extr.setStorageInfo(pnfsMountpoint, pnfsId, storageInfo, accessMode);
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

          String hsmType = getHsmType(mp, dir);
          StorageInfoExtractable extr = getExtractor(hsmType);
          StorageInfo info = extr.getStorageInfo(mp, pnfsId);

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

}
