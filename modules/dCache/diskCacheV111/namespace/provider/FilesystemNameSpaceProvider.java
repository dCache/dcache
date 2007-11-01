/*
 * FilesystemNameSpaceProvider.java
 *
 * Created on March 22, 2005, 5:01 PM
 */

package diskCacheV111.namespace.provider;

/**
 *
 * @author  patrick
 */
import dmg.util.Args;
import dmg.cells.nucleus.CellNucleus;
import diskCacheV111.vehicles.*;
import diskCacheV111.util.*;
import diskCacheV111.namespace.*;

import java.util.*;
import java.io.*;

public class FilesystemNameSpaceProvider extends BasicNameSpaceProvider {
    
    
    
    private File   _db       = null ;
    private CellNucleus _nucleus = null ;
    
    public FilesystemNameSpaceProvider(Args args, CellNucleus nucleus) throws Exception {
        super( args, nucleus);
        _nucleus = nucleus ;    
        try{
        String dbString = args.getOpt("dbURL");
        
        if( dbString == null )
             throw new
             IllegalArgumentException("dbURL not specified");
        
        _db = new File( dbString );
        if( ! _db.isDirectory() )
            throw new
            IllegalArgumentException("dbURL not a directory");
        }catch(Exception e){
            nucleus.say("Problem in FilesystemNameSpaceProvider : "+e);
        }
        
    }
       
    private Set loadCacheLocations( PnfsId pnfsId ) throws IOException {
        File    pnfs = new File( _db ,  pnfsId.toString() ) ;
        HashSet set  = new HashSet() ;
        if( ! pnfs.exists() )return set ;
        
        BufferedReader br = new BufferedReader( new FileReader( pnfs ) ) ;
        try{
             String line = null ;
             while( ( line = br.readLine() ) != null ){
                 line = line.trim() ;
                 if( line.length() == 0 )continue ;
                 set.add(line);
             }
        }finally{
            try{ br.close() ; }catch(Exception ee ){}
        }
        return set ;
    }
    private void storeCacheLocations( PnfsId pnfsId , Set set ) throws IOException {
        File    pnfs = new File( _db ,  pnfsId.toString() ) ;
        if( set.size() == 0 ){pnfs.delete() ; return ; }
        PrintWriter pw = new PrintWriter( new FileWriter( pnfs ) ) ;
        try{
           
            for( Iterator i = set.iterator() ; i.hasNext() ; ){
                String name = i.next().toString() ;
                pw.println(name);
                _nucleus.say("Adding "+name+" to "+pnfs.toString());
            }
            
            
        }finally{
            try{ pw.close() ; }catch(Exception ee ){}
        }
    }
    public void addCacheLocation(PnfsId pnfsId, String cacheLocation) {
        
        
        try {
    
           cacheLocation = cacheLocation.trim() ;
           Set set = loadCacheLocations(pnfsId);
           set.add( cacheLocation);
           storeCacheLocations(pnfsId,set);
           
        }catch( Exception e) {
            e.printStackTrace();
        }
        
    }
    
    public void clearCacheLocation(PnfsId pnfsId, String cacheLocation, boolean removeIfLast) throws Exception {
        
        try {
            
           cacheLocation = cacheLocation.trim() ;
           Set set = loadCacheLocations(pnfsId);
           set.remove( cacheLocation);
           storeCacheLocations(pnfsId,set);
           
        }catch( Exception e) {
            e.printStackTrace();
          
        }
    }
    
    public List getCacheLocation(PnfsId pnfsId) throws Exception{
        
        
        try {
            
            List locations = new Vector( loadCacheLocations(pnfsId) ) ;
            Set set = loadCacheLocations(pnfsId);
            return locations;

        }catch( Exception e) {
            e.printStackTrace();
            throw e ;
        }
        
    }
        
    public String toString() {
        
        return "$Id: FilesystemNameSpaceProvider.java,v 1.2 2005-07-19 11:06:11 patrick Exp $";
        
    }
   
}
