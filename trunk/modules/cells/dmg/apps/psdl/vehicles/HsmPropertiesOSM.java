package dmg.apps.psdl.vehicles ;

import dmg.apps.psdl.pnfs.* ;

import java.io.* ;
import java.util.* ;

public class HsmPropertiesOSM implements HsmProperties {
   private String _store = null , _group = null , _bfid = null  ;
   
   /**
     * extracts the OSM specific stuff from the trash directory
     */
   public HsmPropertiesOSM( File trashDir , PnfsId pnfsId ) throws IOException {
      File dir = new File( trashDir , "1" ) ;
      if( ! dir.isDirectory() )
         throw new IOException( "trash/1 : not a dir." ) ;
      File file = new File( dir , pnfsId.toString() ) ;
      if( ! file.isFile() )
         throw new IOException( "pnfs entry not found : "+file.getAbsolutePath() ) ;
      BufferedReader r = new BufferedReader( new FileReader( file) ) ;
      String line = r.readLine() ;
      r.close() ;
      if( line == null )
         throw new IOException( "trash pnfs entry is empty" ) ;
      
      StringTokenizer st = new StringTokenizer( line ) ;
      if( st.countTokens() < 3 )
         throw new IOException( "trash pnfs entry is empty" ) ;
      _store = st.nextToken() ;
      _group = st.nextToken() ;
      _bfid  = st.nextToken() ;
   }
   public HsmPropertiesOSM( PnfsFile dir , PnfsFile file ) throws IOException {
   
        String [] temp = dir.getTag( "OSMTemplate" ) ;
        if( temp == null )
           throw new IOException( "OSMTemplate not found" ) ;
        _store = null ;
        for( int i = 0 ;i < temp.length ; i++ ){
           StringTokenizer st = new StringTokenizer( temp[i] ) ;
           int tokens = st.countTokens() ;
           if( tokens < 2 )continue ;
           String key = st.nextToken() ;
           String value = st.nextToken() ;
           if( key.equals( "StoreName" ) ){
              _store = value ;
              break ;
           }
        }
        if( _store == null )
           throw new IOException( "No Valid store name found" ) ;
        temp = dir.getTag( "sGroup" ) ;
        if( ( temp == null ) || ( temp.length == 0 ) )
           throw new IOException( "sGroup not found or invalid" ) ;
        _group = temp[0] ;
        
        File level1 = file.getLevelFile( 1 ) ;
        BufferedReader r = new BufferedReader( 
                           new FileReader( level1 ) ) ;
        String line = r.readLine() ;
        r.close() ;
        if( line != null ){
           StringTokenizer st = new StringTokenizer( line ) ;
           if( st.countTokens() > 2 ){
               _store = st.nextToken() ;
               _group = st.nextToken() ;
               _bfid  = st.nextToken() ;
           }
        }
        
   
   }
   public HsmPropertiesOSM( String store , String group ){
     _store = store ;
     _group = group ;
     _bfid  = null ;
   }
   public void setBfid( String bfid ){ _bfid = bfid ; }
   public String getBfid(){ return _bfid ; }
   public String getHsmKey(){
      return _store+"."+_group ;
   }
   public boolean isFile(){ return _bfid != null ; }
   public String getStore(){ return _store ; }
   public String getGroup(){ return _group ; }
   public String toString(){ 
       return _store+":"+_group+":"+(_bfid==null?"NotAFile":_bfid) ;
   }
//   public String getHsmInfo(){
//       return _store +"\n" + _group + "\n" +
//              ( _bfid == null ? "null" : _bfid ) + "\n"  ;
//   
 //  }
   public String getHsmInfo(){
       return _store+":"+_group+":"+(_bfid==null?"null":_bfid) ;
   }
}
 
