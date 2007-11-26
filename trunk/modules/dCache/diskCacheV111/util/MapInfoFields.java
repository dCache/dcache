// $Id: MapInfoFields.java,v 1.1 2002-08-22 06:12:05 cvs Exp $

package diskCacheV111.util ;

import java.util.* ;
import java.io.*;

import dmg.util.Args ;

public class MapInfoFields {

   private String  _mapFile = null ;
   private HashMap _map     = new HashMap() ;
     
   public MapInfoFields( Args args ) throws Exception {
   
      _mapFile = args.getOpt("mapFile") ;
         
      loadMapFile() ;
   }
   public String mapOwner( String owner ){
      if( _mapFile == null )return owner ;
      try{
         String map = (String)_map.get( new Integer( owner ) ) ;
         return map == null ? owner : map ;
      }catch(Exception ee){
         return owner ;
      }
   }
   public String hh_map_ls = "" ;
   public String ac_map_ls_$_0_1( Args args ){
      if( args.argc() == 0 ){
         StringBuffer sb = new StringBuffer() ;
         Iterator i = _map.entrySet().iterator() ;
         while( i.hasNext() ){
            Map.Entry e = (Map.Entry)i.next() ;
            sb.append(e.getKey().toString()).
               append(" ").
               append(e.getValue().toString()).
               append("\n");
         }
         return sb.toString() ;
      }else{
         String key    = args.argv(0);
         String result = (String)_map.get(new Integer(key));
         return result == null ? ( "Not Found : "+key ) : result ;
      }
   }
   public String hh_map_reload = "" ;
   public String ac_map_reload( Args args ) throws Exception {
       loadMapFile() ;
       return "" ;
   }
   private void loadMapFile() throws Exception {
   
      if( _mapFile == null )
         throw new
         IllegalArgumentException("No Map File defined");
         
      _map.clear() ;
      
      BufferedReader br = new BufferedReader(
                             new FileReader(_mapFile));
      
      try{
         String line = null ;
         while( ( line = br.readLine() ) != null ){
            StringTokenizer st = new StringTokenizer(line,":") ;
            try{
              String name = st.nextToken() ;
              st.nextToken() ;
              _map.put( new Integer( st.nextToken() ) , name ) ;               
            }catch(Exception ie ){}
         }
      }catch(Exception ee ){
         throw ee ;
      }finally{
         try{ br.close() ;}catch(Exception eee){}
      }
   }
    
}
