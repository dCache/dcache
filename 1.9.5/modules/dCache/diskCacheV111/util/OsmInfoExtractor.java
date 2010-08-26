package diskCacheV111.util ;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import diskCacheV111.namespace.NameSpaceProvider;
import diskCacheV111.vehicles.OSMStorageInfo;
import diskCacheV111.vehicles.StorageInfo;
import java.io.File;
import org.dcache.util.AbstractPnfsExtractor;

public class OsmInfoExtractor extends AbstractPnfsExtractor {

    @Override
    public void setStorageInfo( String pnfsMountpoint , PnfsId pnfsId ,
                                StorageInfo storageInfo , int accessMode )
           throws CacheException {
       //
       // access mode
       //     0 exclusive ( don't write if exists )
       //     1 overwrite
       //     2 append
       //

       PnfsFile pnfsFile = PnfsFile.getFileByPnfsId( pnfsMountpoint , pnfsId ) ;
       if( pnfsFile == null ) {
          throw new
          CacheException( 37 , "Not a valid PnfsId "+pnfsId ) ;
       }

       if( storageInfo.isSetBitFileId() ) {

           OSMStorageInfo osm = (OSMStorageInfo)storageInfo ;

	       File levelFile = pnfsFile.getLevelFile(1) ;

	       switch( accessMode ){
	          case  NameSpaceProvider.SI_EXCLUSIVE :
	             if( levelFile.length() > 0 ) {
	                throw new
	                CacheException( 38 , "File already exits (can't overwrite mode=0)" ) ;
	             }
	          case NameSpaceProvider.SI_APPEND :
	          case NameSpaceProvider.SI_OVERWRITE :
	             try{
	                PrintWriter pw =  null;
	                try{

	                	pw = new PrintWriter( new FileWriter( levelFile ,
		                        accessMode == NameSpaceProvider.SI_APPEND ) ) ;

	                    pw.println( osm.getStore()+" "+
	                                osm.getStorageGroup()+" "+
	                                osm.getBitfileId() ) ;
	                    pw.flush();
	                }finally{
	                   if( pw != null )  pw.close() ;
	                }

	             }catch(IOException ee ){
	                throw new
	                CacheException(107,"Problem in set(OSM)StorageInfo : "+ee ) ;
	             }
	             break ;
	          default :
	             throw new
	             CacheException( 39 , "Illegal Access Mode : "+accessMode ) ;
	       }
       }

       if( storageInfo.isSetAddLocation() ) {

	       File levelFile = pnfsFile.getLevelFile(1) ;

	       switch( accessMode ){
	          case  NameSpaceProvider.SI_EXCLUSIVE :
	             if( levelFile.length() > 0 ) {
	                throw new
	                CacheException( 38 , "File already exits (can't overwrite mode=0)" ) ;
	             }
	          case NameSpaceProvider.SI_APPEND :
	          case NameSpaceProvider.SI_OVERWRITE :
	             try {

					List<URI> newLocations = storageInfo.locations();
					PrintWriter pw = null;

					try {
						pw = new PrintWriter(new FileWriter(levelFile,
								accessMode == NameSpaceProvider.SI_APPEND));

						for (URI locationURI : newLocations) {
							pw.println(new OsmLocationExtractor(locationURI).toLevels().get(1));
						}

						if( pw.checkError() ) {
							throw new IOException("Failed to flush data into leve1");
						}

					} finally {
						if (pw != null) pw.close();
					}


	             }catch(IOException ee ){
	                throw new
	                CacheException(107,"Problem in set(OSM)StorageInfo : "+ee ) ;
	             }
	             break ;
	          default :
	             throw new
	             CacheException( 39 , "Illegal Access Mode : "+accessMode ) ;
	       }
       }

       return ;
    }

    @Override
    protected StorageInfo extractDirectory( String mp , PnfsFile x )
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
           String [] group    = parentDir.getTag("sGroup" ) ;
           String [] spaceToken = parentDir.getTag("WriteToken");

           if( ( template == null     ) || ( group == null     ) ||
               ( template.length == 0 ) || ( group.length == 0 ) )
              throw new
              CacheException(
                35, "OSM info not found in "+parentDir+"(type="+
                parentDir.getPnfsFileType()+")" ) ;

           Map<String, String>       hash = new HashMap<String, String>() ;
           StringTokenizer st   = null ;
           for( int i = 0 ; i < template.length ; i++ ){
              st = new StringTokenizer( template[i] ) ;
              if( st.countTokens() < 2 )continue ;
              hash.put( st.nextToken() , st.nextToken() ) ;
           }
           String store = hash.get("StoreName") ;
           if( store == null )
                 throw new
                 CacheException( 37 , "StoreName not found in template" ) ;

           String gr    = group[0].trim() ;

           OSMStorageInfo info = new OSMStorageInfo( store , gr ) ;
           info.addKeys( hash ) ;

           // overwrite hsm type with hsmInstance tag
           String [] tag = parentDir.getTag("hsmInstance") ;
           if( (tag != null ) && ( tag.length > 0 ) &&  ( tag[0] != null ) ) {
               info.setHsm( tag[0].trim().toLowerCase());
           }

           if( spaceToken != null ) {
               info.setKey("writeToken", spaceToken[0].trim());
           }

           return info ;
    }

    @Override
    protected StorageInfo extractFile(String mp, PnfsFile pnfsFile)
        throws CacheException
    {
        StorageInfo storageInfo;

        File level = pnfsFile.getLevelFile(1) ;
        if( level.length() == 0 ){
           //
           // file seems not be in HSM.
           // We need to get the stuff from the directory.
           //
           storageInfo = extractDirectory( mp , pnfsFile ) ;
        }else{
            List<String> levelContent = super.readLines(level);

            assert !levelContent.isEmpty();

            String[] fields = levelContent.get(0).split("[ \t]") ;
            if (fields.length < 3) {
                throw new CacheException(38,
                        "Level 1 content of " + pnfsFile.getPnfsId() + " is invalid [" + levelContent.get(0) + "]");
            }

            /*
             * first three filelds of level1 used to construct Storage info
             */
            storageInfo = new OSMStorageInfo(fields[0], fields[1], fields[2]);

            for(String levelEntry: levelContent) {
                levelEntry = levelEntry.trim();
                if( levelEntry.length() > 0 ) {
                    storageInfo.addLocation(OsmLocationExtractor.parseLevel(levelEntry));
                }
            }

        }

        return storageInfo ;

    }

}
