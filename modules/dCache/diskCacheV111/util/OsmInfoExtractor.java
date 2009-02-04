package diskCacheV111.util ;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import diskCacheV111.namespace.StorageInfoProvider;
import diskCacheV111.vehicles.CacheInfo;
import diskCacheV111.vehicles.OSMStorageInfo;
import diskCacheV111.vehicles.StorageInfo;

public class       OsmInfoExtractor
       implements StorageInfoExtractable {

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

       storeAlRpInLevel2(storageInfo, pnfsFile);

       if( storageInfo.isSetBitFileId() ) {

           OSMStorageInfo osm = (OSMStorageInfo)storageInfo ;

	       File levelFile = pnfsFile.getLevelFile(1) ;

	       switch( accessMode ){
	          case  StorageInfoProvider.SI_EXCLUSIVE :
	             if( levelFile.length() > 0 ) {
	                throw new
	                CacheException( 38 , "File already exits (can't overwrite mode=0)" ) ;
	             }
	          case StorageInfoProvider.SI_APPEND :
	          case StorageInfoProvider.SI_OVERWRITE :
	             try{
	                PrintWriter pw =  null;
	                try{

	                	pw = new PrintWriter( new FileWriter( levelFile ,
		                        accessMode == StorageInfoProvider.SI_APPEND ) ) ;

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
	          case  StorageInfoProvider.SI_EXCLUSIVE :
	             if( levelFile.length() > 0 ) {
	                throw new
	                CacheException( 38 , "File already exits (can't overwrite mode=0)" ) ;
	             }
	          case StorageInfoProvider.SI_APPEND :
	          case StorageInfoProvider.SI_OVERWRITE :
	             try {

					List<URI> newLocations = storageInfo.locations();
					PrintWriter pw = null;

					try {
						pw = new PrintWriter(new FileWriter(levelFile,
								accessMode == StorageInfoProvider.SI_APPEND));

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
    
    /**
	 * 	HACK -  AccessLatency and RetentionPolicy stored as a flags
     * FIXME: this information shouldn't be stored here.
     * @param storageInfo
     * @param pnfsFile
     * @throws CacheException
     */
	protected void storeAlRpInLevel2(StorageInfo storageInfo, PnfsFile pnfsFile)
			throws CacheException {
		   try {
		       CacheInfo info   = new CacheInfo( pnfsFile ) ;
		       CacheInfo.CacheFlags flags = info.getFlags() ;

		       if( storageInfo.isSetAccessLatency() ) {
		           flags.put( "al" , storageInfo.getAccessLatency().toString() ) ;
		       }

		       if(storageInfo.isSetRetentionPolicy() ) {
		    	   flags.put( "rp" , storageInfo.getRetentionPolicy().toString() ) ;
		       }

		       info.writeCacheInfo( pnfsFile ) ;

		   }catch(IOException ee ){
		       throw new
		       CacheException(107,"Problem in set(OSM)StorageInfo : "+ee ) ;
		    }
	}
	
    public StorageInfo getStorageInfo( String mp , PnfsId pnfsId )
           throws CacheException {
       try{
          PnfsFile x = PnfsFile.getFileByPnfsId( mp , pnfsId ) ;
          if( x == null ){
             throw new
             CacheException( 37 , "Not a valid PnfsId "+pnfsId ) ;
          }else if( x.isDirectory() ){
              return extractDirectory( mp , x ) ;
          }else if( x.isFile() ){
              return extractFile( mp , x ) ;
          }else if( x.isLink() ){
              return getStorageInfo( mp , new PnfsFile( x.getCanonicalPath() ).getPnfsId() ) ;
          }else{
             throw new
             CacheException( 34 , "Can't file "+pnfsId ) ;
          }
       }catch(CacheException ce ){
          throw ce ;
       }catch(Exception e ){
          e.printStackTrace();
          throw new
          CacheException( 33 , "unexpected : "+e ) ;
       }

    }
    protected OSMStorageInfo extractDirectory( String mp , PnfsFile x )
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
           String [] accessLatency = parentDir.getTag("AccessLatency");
           String [] retentionPolicy = parentDir.getTag("RetentionPolicy");
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

           /*
            * if Access latency and/or retention policy is defined for a directory
            * apply it to the file and make it persistent, while it's a file attribute and directory
            * tag is default value only
            */
           if(accessLatency != null) {
        	   try {
        		   info.setAccessLatency( AccessLatency.getAccessLatency(accessLatency[0].trim()));
        		   info.isSetAccessLatency(true);
        	   }catch(IllegalArgumentException iae) {
        		   // TODO: do we fail here or not?
        	   }
           }

           if(retentionPolicy != null) {
        	   try {
        		   info.setRetentionPolicy( RetentionPolicy.getRetentionPolicy(retentionPolicy[0].trim()));
        		   info.isSetRetentionPolicy(true);
        	   }catch(IllegalArgumentException iae) {
        		   // TODO: do we fail here or not?
        	   }
           }

           if( spaceToken != null ) {
               info.setKey("writeToken", spaceToken[0].trim());
           }

           return info ;
    }

    private StorageInfo extractFile(String mp, PnfsFile pnfsFile)
        throws Exception
    {
        OSMStorageInfo storageInfo;

        File level = pnfsFile.getLevelFile(1) ;
        if( level.length() == 0 ){
           //
           // file seems not be in HSM.
           // We need to get the stuff from the directory.
           //
           storageInfo = extractDirectory( mp , pnfsFile ) ;
        }else{
           BufferedReader br = null ;
           try{
              br = new BufferedReader(new FileReader(level));
              String line = br.readLine() ;
              if ((line == null) || line.length() == 0) {
                  throw new
                      CacheException(37, "Level 1 content of "
                                     + pnfsFile.getPnfsId() + " is invalid");
              }

              StringTokenizer st = new StringTokenizer(line);
              if (st.countTokens() < 3) {
                  throw new CacheException(38, "Level 1 content of "
                                           + pnfsFile.getPnfsId()
                                           + " is invalid(2)=[" + line + "]");
              }

              storageInfo =
                  new OSMStorageInfo(st.nextToken(),
                                     st.nextToken(),
                                     st.nextToken());

               Map<Integer,String> levels = new HashMap<Integer,String>(1);
               levels.put(1, line);
               HsmLocation location = new OsmLocationExtractor(levels);
               storageInfo.addLocation(location.location());

               while ((line = br.readLine()) != null) {
                   if (line.length() != 0) {
                       levels.put(1, line);
                       location = new OsmLocationExtractor(levels);
                       storageInfo.addLocation(location.location());
                   }
               }

           } catch (IllegalArgumentException e) {
               throw new
                   CacheException(38, "Level 1 content of "
                                  + pnfsFile.getPnfsId()
                                  + " is invalid (" + e.getMessage() + ")");
           } finally {
               try {
                   if (br != null) {
                       br.close();
                   }
               } catch(Exception ie) {
                   /* to late to react */
               }
           }
        }
        storageInfo.setFileSize( pnfsFile.length() ) ;
        storageInfo.setIsNew( ( pnfsFile.length() == 0 ) && ( pnfsFile.getLevelFile(2).length() == 0 ) );
        storageInfo.setIsStored( level.length() > 0 ) ;

        // FIXME: HACK -  AccessLatency and RetentionPolicy stored as a flags
        try {
 	       CacheInfo cacheInfo   = new CacheInfo( pnfsFile ) ;
 	       CacheInfo.CacheFlags flags = cacheInfo.getFlags() ;

 	       String al = flags.get("al");
 	       if( al != null ) {
 	    	  storageInfo.setAccessLatency( AccessLatency.getAccessLatency(al) );
 	       }

 	       String rp = flags.get("rp");
 	       if(rp != null ) {
 	    	  storageInfo.setRetentionPolicy(RetentionPolicy.getRetentionPolicy(rp) );
 	       }


        }catch(IOException ee ){
            throw new
            CacheException(107,"Problem in get(OSM)StorageInfo : "+ee ) ;
         }


        return storageInfo ;

    }
    public static void main( String [] args ) throws Exception {
       if( args.length < 2 ){
          System.err.println( "Usage : ... <mp> <pnfsId>" ) ;
          System.exit(4);
       }
       StorageInfoExtractable sie = new OsmInfoExtractor() ;

       StorageInfo info = sie.getStorageInfo( args[0] , new PnfsId(args[1]) ) ;
       System.out.println( "["+info.getClass()+"]="+info.toString() ) ;
       System.exit(0);
    }

}
