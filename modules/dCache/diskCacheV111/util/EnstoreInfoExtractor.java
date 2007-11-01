package diskCacheV111.util ;

import  diskCacheV111.vehicles.* ;

import  java.util.* ;
import  java.io.* ;
public class       EnstoreInfoExtractor 
       implements  StorageInfoExtractable {   

   public void setStorageInfo( String pnfsMountpoint , PnfsId pnfsId ,
                               StorageInfo storageInfo , int accessMode )
           throws CacheException {


	   PnfsFile pnfsFile = PnfsFile.getFileByPnfsId( pnfsMountpoint , pnfsId ) ;
	   
       // FIXME: HACK -  AccessLatency and RetentionPolicy stored as a flags 
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
       
       if( storageInfo.isSetBitFileId() ) {
    	   throw new
           CacheException( 1 , "Operation not suppported : setStorageInfo" ) ;    	   
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
          }else
             throw new 
             CacheException( 34 , "Can't find file "+pnfsId ) ;
       }catch(CacheException ce ){
          throw ce ;
       }catch(Exception e ){
          e.printStackTrace();
          throw new
          CacheException( 33 , "unexpected : "+e ) ;
       }
    
    }
    private EnstoreStorageInfo extractDirectory( String mp , PnfsFile x )
            throws Exception {
            
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
           String [] accessLatency = parentDir.getTag("AccessLatency");
           String [] retentionPolicy = parentDir.getTag("RetentionPolicy");
           
           
           if( ( family == null     ) || ( family.length == 0 ) )
              throw new
              CacheException( 
                35, "Enstore info not found in "+parentDir+"(type="+
                parentDir.getPnfsFileType()+")" ) ;
                
           Hashtable       hash = new Hashtable() ;
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
           
           
           return info ;
    }
     
    private static final long TWOGIG = 2L*1024*1024*1024;
    private StorageInfo extractFile( String mp , PnfsFile x )
            throws Exception {
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
           String line     = null ;
           String bfid     = null ;
           String volume   = null ;
           String location = null ;
           String sizeStr  = null ;
           String family   = null ;
           BufferedReader br = new BufferedReader(  new FileReader( level ) ) ;
           try{
              for( int i =  0 ; ; i++ ){
                  try{ 
                      if( ( line = br.readLine() ) == null )break ;
                  }catch(IOException ioe ){
                      break ;
                  }
                  switch(i){
                     case 0 : volume    = line ; break ;
                     case 1 : location  = line ; break ;
                     case 2 : sizeStr   = line ; break ;
                     case 3 : family    = line ; break ;
                     case 8 : bfid      = line ; break ;
                  }
              }
           }finally{
              try{ br.close() ; }catch(Exception ie ){}
           }
           if( ( family == null ) || ( bfid == null ) )
             throw new
             CacheException( 37 , 
             "Level 4 content of "+x.getPnfsId()+" is invalid (nobfid)" ) ;
           if(sizeStr != null) {
               try {
                   //convert enstore size into long
                   long enstoreFileSize = Long.parseLong(sizeStr);
                   // if it is a special case, when layer two is empty and file size is
                   // 2 GB or greater, (which is stored as just 1 in pnfs)
                   // we use enstore file size
                   if(fileSize != enstoreFileSize && fileSize == 1 && enstoreFileSize >= TWOGIG) {
                       fileSize = enstoreFileSize;
                   }
               } catch (java.lang.NumberFormatException nfm) {
                   //enstore size is not parsable
               }
           }
           family = family.trim() ;
           bfid   = bfid.trim() ;
           EnstoreStorageInfo helper = extractDirectory( mp , x ) ;

           info = new EnstoreStorageInfo( helper.getStorageGroup() ,
                                          family ,
                                          bfid ) ;  
           info.setVolume( volume ) ;
           info.setLocation( location ) ;        
        }

        info.setFileSize( fileSize ) ;
        info.setIsNew( ( x.length() == 0 ) && ( x.getLevelFile(2).length() == 0 ) );
        info.setIsStored( level.length() > 0 );
        
        // FIXME: HACK -  AccessLatency and RetentionPolicy stored as a flags 
        try {
 	       CacheInfo cacheInfo   = new CacheInfo( x ) ;
 	       CacheInfo.CacheFlags flags = cacheInfo.getFlags() ;
 	       
 	       String al = flags.get("al");
 	       if( al != null ) {
 	    	  info.setAccessLatency( AccessLatency.getAccessLatency(al) ); 
 	       }
 	       
 	       String rp = flags.get("rp");
 	       if(rp != null ) {
 	    	  info.setRetentionPolicy(RetentionPolicy.getRetentionPolicy(rp) );
 	       } 	       
 	       
        }catch(IOException ee ){
            throw new 
            CacheException(107,"Problem in set(OSM)StorageInfo : "+ee ) ;
         }        
        
        return info ;
            
    }
    public static void main( String [] args ) throws Exception {
       if( args.length < 2 ){
          System.err.println( "Usage : ... <mp> <pnfsId>" ) ;
          System.exit(4);
       }
       StorageInfoExtractable sie = new EnstoreInfoExtractor() ;
       
       StorageInfo info = sie.getStorageInfo( args[0] , new PnfsId(args[1]) ) ;
       System.out.println( "java class        = ["+info.getClass().getName()+"]") ;
       System.out.println( "object.toString() = "+info.toString() ) ;
       System.out.println( "storage class     = "+info.getStorageClass() ) ;
       System.exit(0);
    }

}
