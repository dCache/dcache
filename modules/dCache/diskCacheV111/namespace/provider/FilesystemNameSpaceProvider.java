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
import diskCacheV111.util.*;

import java.util.*;
import java.io.*;
import javax.security.auth.Subject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FilesystemNameSpaceProvider extends BasicNameSpaceProvider {

    private final static Logger _log =
        LoggerFactory.getLogger(FilesystemNameSpaceProvider.class);

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
            _log.info("Problem in FilesystemNameSpaceProvider : "+e);
        }

    }

    private Set<String> loadCacheLocations( PnfsId pnfsId ) throws IOException {
        File    pnfs = new File( _db ,  pnfsId.toString() ) ;
        HashSet<String> set  = new HashSet<String>() ;
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
    private void storeCacheLocations( PnfsId pnfsId , Set<String> set ) throws IOException {
        File    pnfs = new File( _db ,  pnfsId.toString() ) ;
        if( set.size() == 0 ){pnfs.delete() ; return ; }
        PrintWriter pw = new PrintWriter( new FileWriter( pnfs ) ) ;
        try{

            for( String name:  set ){

                pw.println(name);
                _log.info("Adding "+name+" to "+pnfs.toString());
            }


        }finally{
            try{ pw.close() ; }catch(Exception ee ){}
        }
    }

    public void addCacheLocation(Subject subject, PnfsId pnfsId, String cacheLocation)
        throws CacheException
    {
        try {
           cacheLocation = cacheLocation.trim() ;
           Set<String> set = loadCacheLocations(pnfsId);
           set.add( cacheLocation);
           storeCacheLocations(pnfsId,set);
        } catch (IOException e) {
            throw new CacheException(CacheException.UNEXPECTED_SYSTEM_EXCEPTION,
                                     e.getMessage());
        }
    }

    public void clearCacheLocation(Subject subject, PnfsId pnfsId, String cacheLocation, boolean removeIfLast)
        throws CacheException
    {
        try {
            cacheLocation = cacheLocation.trim() ;
            Set<String> set = loadCacheLocations(pnfsId);
            set.remove( cacheLocation);
            storeCacheLocations(pnfsId,set);
        } catch (IOException e) {
            throw new CacheException(CacheException.UNEXPECTED_SYSTEM_EXCEPTION,
                                     e.getMessage());
        }
    }

    public List<String> getCacheLocation(Subject subject, PnfsId pnfsId) throws CacheException
    {
        try {
            return new Vector<String>( loadCacheLocations(pnfsId) ) ;
        } catch (IOException e) {
            throw new CacheException(CacheException.UNEXPECTED_SYSTEM_EXCEPTION,
                                     e.getMessage());
        }
    }

    public String toString() {

        return "$Id: FilesystemNameSpaceProvider.java,v 1.3 2007-05-24 13:51:13 tigran Exp $";

    }

}
