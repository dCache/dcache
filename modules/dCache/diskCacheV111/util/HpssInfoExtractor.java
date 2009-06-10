package diskCacheV111.util ;

import  diskCacheV111.vehicles.* ;

import  java.util.* ;
import  java.io.* ;
import org.dcache.util.AbstractPnfsExtractor;

public class HpssInfoExtractor extends AbstractPnfsExtractor {

    public HpssInfoExtractor(AccessLatency defaultAL, RetentionPolicy defaultRP) {
       super(defaultAL, defaultRP);
    }

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

       PnfsFile x = PnfsFile.getFileByPnfsId( pnfsMountpoint , pnfsId ) ;
       if( x == null )
          throw new
          CacheException( 37 , "Not a valid PnfsId "+pnfsId ) ;

          super.storeAlRpInLevel2(storageInfo, x);

       if( storageInfo.isSetBitFileId() ) {

           HpssStorageInfo hpss = (HpssStorageInfo)storageInfo ;

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
           }else{
               // force default
               info.setAccessLatency(_defaultAccessLatency);
           }

           if(retentionPolicy != null) {
        	   try {
        		   info.setRetentionPolicy( RetentionPolicy.getRetentionPolicy(retentionPolicy[0].trim()));
        		   info.isSetRetentionPolicy(true);
        	   }catch(IllegalArgumentException iae) {
        		   // TODO: do we fail here or not?
        	   }
           }else{
               // force default
               info.setRetentionPolicy(_defaultRetentionPolicy);
           }

           if( spaceToken != null ) {
               info.setKey("writeToken", spaceToken[0].trim());
           }

           return info ;
    }

    @Override
    protected StorageInfo extractFile( String mp , PnfsFile x )
            throws CacheException {

        StorageInfo info = null ;
        File level = x.getLevelFile(1) ;
        if( level.length() == 0 ){
           //
           // file seems not be in HSM.
           // We need to get the stuff from the directory.
           //
           info = extractDirectory( mp , x ) ;
        }else{
             List<String> levelContent = super.readLines(level);

            assert !levelContent.isEmpty();

            StringTokenizer st = new StringTokenizer( levelContent.get(0) ) ;
            if (st.countTokens() < 3) {
                throw new CacheException(38,
                        "Level 1 content of " + x.getPnfsId() + " is invalid [" + levelContent.get(0) + "]");
            }

              info =
               new HpssStorageInfo( st.nextToken() ,
                                   st.nextToken() ,
                                   st.nextToken()   ) ;
        }

        return info ;

    }

}
