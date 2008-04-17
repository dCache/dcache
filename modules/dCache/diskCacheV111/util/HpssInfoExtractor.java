package diskCacheV111.util ;

import  diskCacheV111.vehicles.* ;

import  java.util.* ;
import  java.io.* ;
public class       HpssInfoExtractor
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

       HpssStorageInfo hpss = (HpssStorageInfo)storageInfo ;

       PnfsFile x = PnfsFile.getFileByPnfsId( pnfsMountpoint , pnfsId ) ;
       if( x == null )
          throw new
          CacheException( 37 , "Not a valid PnfsId "+pnfsId ) ;


       // FIXME: HACK -  AccessLatency and RetentionPolicy stored as a flags
       try {
	       CacheInfo info   = new CacheInfo( x ) ;
	       CacheInfo.CacheFlags flags = info.getFlags() ;

	       if( storageInfo.isSetAccessLatency() ) {
	           flags.put( "al" , storageInfo.getAccessLatency().toString() ) ;
	       }

	       if(storageInfo.isSetRetentionPolicy() ) {
	    	   flags.put( "rp" , storageInfo.getRetentionPolicy().toString() ) ;
	       }

           info.writeCacheInfo( x ) ;

       }catch(IOException ee ){
           throw new
           CacheException(107,"Problem in set(OSM)StorageInfo : "+ee ) ;
        }


       if( storageInfo.isSetBitFileId() ) {

	       File levelFile = x.getLevelFile(1) ;

	       switch( accessMode ){
	          case  0 :
	             if( levelFile.length() > 0 )
	                throw new
	                CacheException( 38 , "File already exits (can't overwrite mode=0)" ) ;
	          case 1 :
	          case 2 :
	             try{
	                PrintWriter pw = new PrintWriter( new FileWriter( levelFile , accessMode == 2 ) ) ;
	                try{
	                    pw.println( hpss.getStore()+" "+
	                                hpss.getStorageGroup()+" "+
	                                hpss.getBitfileId() ) ;
	                    pw.flush();
	                }finally{
	                   try{ pw.close() ; }catch(Exception eee){}
	                }

	             }catch(IOException ee ){
	                throw new
	                CacheException(107,"Problem in set(Hpss)StorageInfo : "+ee ) ;
	             }
	             break ;
	          default :
	             throw new
	             CacheException( 39 , "Illegal Access Mode : "+accessMode ) ;
	       }

       }
       return ;
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
             CacheException( 34 , "Can't file "+pnfsId ) ;
       }catch(CacheException ce ){
          throw ce ;
       }catch(Exception e ){
          e.printStackTrace();
          throw new
          CacheException( 33 , "unexpected : "+e ) ;
       }

    }
    private HpssStorageInfo extractDirectory( String mp , PnfsFile x )
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


           String [] template = parentDir.getTag("HpssTemplate") ;
           String [] group    = parentDir.getTag("sGroup" ) ;
           String [] accessLatency = parentDir.getTag("AccessLatency");
           String [] retentionPolicy = parentDir.getTag("RetentionPolicy");
           String [] spaceToken = parentDir.getTag("WriteToken");

           if( ( template == null     ) || ( group == null     ) ||
               ( template.length == 0 ) || ( group.length == 0 ) )
              throw new
              CacheException(
                35, "Hpss info not found in "+parentDir+"(type="+
                parentDir.getPnfsFileType()+")" ) ;

           Hashtable       hash = new Hashtable() ;
           StringTokenizer st   = null ;
           for( int i = 0 ; i < template.length ; i++ ){
              st = new StringTokenizer( template[i] ) ;
              if( st.countTokens() < 2 )continue ;
              hash.put( st.nextToken() , st.nextToken() ) ;
           }
           String store = (String)hash.get("StoreName") ;
           if( store == null )
                 throw new
                 CacheException( 37 , "StoreName not found in template" ) ;

           String gr    = group[0].trim() ;

           HpssStorageInfo info = new HpssStorageInfo( store , gr ) ;
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

           if( spaceToken != null ) {
               info.setKey("writeToken", spaceToken[0].trim());
           }

           return info ;
    }

    private StorageInfo extractFile( String mp , PnfsFile x )
            throws Exception {

        HpssStorageInfo info = null ;
        File level = x.getLevelFile(1) ;
        if( level.length() == 0 ){
           //
           // file seems not be in HSM.
           // We need to get the stuff from the directory.
           //
           info = extractDirectory( mp , x ) ;
        }else{
           BufferedReader br = null ;
           try{
              br = new BufferedReader(
                      new FileReader( level ) ) ;
              String line = br.readLine() ;
              if( ( line == null ) ||  ( line.length() == 0 ) )
                throw new
                CacheException( 37 ,
                "Level 1 content of "+x.getPnfsId()+" is invalid" ) ;

              StringTokenizer st = new StringTokenizer( line ) ;
              if( st.countTokens() < 3 )
                throw new
                CacheException( 38 ,
                "Level 1 content of "+x.getPnfsId()+" is invalid(2)=["+line+"]" ) ;

              info =
               new HpssStorageInfo( st.nextToken() ,
                                   st.nextToken() ,
                                   st.nextToken()   ) ;

           }finally{
             try{ br.close() ; }catch(Exception ie ){}
           }
        }
        info.setFileSize( x.length() ) ;
        info.setIsNew( ( x.length() == 0 ) && ( x.getLevelFile(2).length() == 0 ) );
        info.setIsStored( level.length() > 0 ) ;

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
            CacheException(107,"Problem in get(OSM)StorageInfo : "+ee ) ;
        }

        return info ;

    }
    public static void main( String [] args ) throws Exception {
       if( args.length < 2 ){
          System.err.println( "Usage : ... <mp> <pnfsId>" ) ;
          System.exit(4);
       }
       StorageInfoExtractable sie = new HpssInfoExtractor() ;

       StorageInfo info = sie.getStorageInfo( args[0] , new PnfsId(args[1]) ) ;
       System.out.println( "["+info.getClass()+"]="+info.toString() ) ;
       System.exit(0);
    }

}
