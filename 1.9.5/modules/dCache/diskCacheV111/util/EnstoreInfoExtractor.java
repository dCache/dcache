package diskCacheV111.util ;

import  diskCacheV111.vehicles.* ;

import  java.util.* ;
import  java.io.* ;
import org.apache.log4j.Logger;
import org.dcache.util.AbstractPnfsExtractor;

public class  EnstoreInfoExtractor extends AbstractPnfsExtractor {

    private static final Logger _log = Logger.getLogger(EnstoreInfoExtractor.class);

    @Override
   public void setStorageInfo( String pnfsMountpoint , PnfsId pnfsId ,
                               StorageInfo storageInfo , int accessMode )
           throws CacheException {

       if( storageInfo.isSetBitFileId() ) {
    	   throw new
           CacheException( 1 , "Operation not suppported : setStorageInfo" ) ;
       }

    }

   @Override
    protected EnstoreStorageInfo extractDirectory( String mp , PnfsFile x )
            throws CacheException {

           PnfsFile parentDir = null ;
           if( x.isDirectory() ){
               parentDir = x ;
           }else{
               PnfsId parent = x.getParentId() ;
               if( parent == null )
                  throw new
                  CacheException( 36, "Couldn't determine parent ID" ) ;
                parentDir = PnfsFile.getFileByPnfsId( mp , parent ) ;
           }

           String [] template = parentDir.getTag("OSMTemplate") ;
           String [] group    = parentDir.getTag("storage_group" ) ;
           String [] family   = parentDir.getTag("file_family" ) ;
           String [] spaceToken = parentDir.getTag("WriteToken");


           if( ( family == null     ) || ( family.length == 0 ) )
              throw new
              CacheException(
                35, "Enstore info not found in "+parentDir+"(type="+
                parentDir.getPnfsFileType()+")" ) ;

           Map<String, String>       hash = new HashMap<String, String>() ;
           if( template != null ){
              StringTokenizer st   = null ;
              for( int i = 0 ; i < template.length ; i++ ){
                 st = new StringTokenizer( template[i] ) ;
                 if( st.countTokens() < 2 )continue ;
                 hash.put( st.nextToken() , st.nextToken() ) ;
              }
           }
           String gr = ( group        == null ) ||
                       ( group.length == 0    ) ||
                       ( ( gr = group[0].trim() ).equals("") ) ? "None" : gr ;


           EnstoreStorageInfo info = new EnstoreStorageInfo( gr , family[0].trim() ) ;
           info.addKeys( hash ) ;

           if( spaceToken != null ) {
               info.setKey("writeToken", spaceToken[0].trim());
           }

           return info ;
    }

    private static final long TWOGIG = 2L*1024*1024*1024;

    @Override
    protected StorageInfo extractFile( String mp , PnfsFile x )
            throws CacheException {
        EnstoreStorageInfo info = null ;
        File level = x.getLevelFile(4) ;

        long fileSize = x.length();
        if( level.length() == 0 ){
           //
           // not yet in HSM.
           // We need to get the stuff from the directory.
           //
           info = extractDirectory( mp , x ) ;
        }else{

            List<String> levelContent = super.readLines(level);

            if (levelContent.size() < 9) {
                throw new CacheException(37,
                        "Level 4 content of " + x.getPnfsId() + " is invalid (nobfid)");
            }

            String volume = levelContent.get(0);
            String location = levelContent.get(1);
            String sizeStr = levelContent.get(2);
            String family = levelContent.get(3);
            String bfid = levelContent.get(8);

            try {
                //convert enstore size into long
                long enstoreFileSize = Long.parseLong(sizeStr);
                // if it is a special case, when layer two is empty and file size is
                // 2 GB or greater, (which is stored as just 1 in pnfs)
                // we use enstore file size
                if (fileSize != enstoreFileSize && fileSize == 1 && enstoreFileSize >= TWOGIG) {
                    fileSize = enstoreFileSize;
                } else if ( fileSize != enstoreFileSize && fileSize != 1) {
                    _log.warn(String.format("File size mismatch: enstore=%d, pnfs=%d",
                            enstoreFileSize, fileSize) );
                }
            } catch (java.lang.NumberFormatException nfm) {
                _log.warn("File size entry in level4 is not a valid number: " + nfm.getMessage());
            }

           family = family.trim() ;
           bfid   = bfid.trim() ;
           EnstoreStorageInfo helper = extractDirectory( mp , x ) ;

           info = new EnstoreStorageInfo( helper.getStorageGroup() ,
                                          family ,
                                          bfid ) ;
           info.setVolume( volume ) ;
           info.setLocation( location ) ;
           info.setFileSize(fileSize);
        }

        return info ;
    }
}
