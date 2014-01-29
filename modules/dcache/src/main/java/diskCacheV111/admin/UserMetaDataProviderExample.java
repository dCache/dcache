package diskCacheV111.admin ;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import dmg.cells.nucleus.CellAdapter;
import dmg.util.CollectionFactory;

import org.dcache.util.Args;

/**
 * Author : Patrick Fuhrmann
 */
public class UserMetaDataProviderExample implements UserMetaDataProvider {

    private final CellAdapter _cell;
    private final Map<String,Object> _context;
    private final Args        _args;
    private final String      _ourName;

    private String  _baseDirectoryName;
    private File    _baseDir;

    private int     _requestCount;
    private final Map<String, Integer> _userStatistics =
        CollectionFactory.newHashMap();
    /**
      * we are assumed to provide the folling contructor signature.
      */
    public UserMetaDataProviderExample( CellAdapter cell ){

       _cell    =  cell ;
       _context = _cell.getDomainContext() ;
       _args    = _cell.getArgs() ;
       _ourName = this.getClass().getName() ;
       //
       //
       //  get some information from the
       //  command line or the domain context.
       //
       _baseDirectoryName = (String)_context.get("umd-mapping") ;
       _baseDirectoryName = _baseDirectoryName == null ?
                            _args.getOpt("umd-mapping") :
                            _baseDirectoryName ;

      if( _baseDirectoryName == null ) {
          throw new
                  IllegalArgumentException(_ourName + " : Base directory not specified");
      }

      _baseDir = new File( _baseDirectoryName ) ;
      if( ! _baseDir.isDirectory() ) {
          throw new
                  IllegalArgumentException(
                  _ourName + " : not a directory : " + _baseDirectoryName);
      }

    }
    /**
      * just for the fun of it
      */
    public static final String hh_ls = "" ;
    public String ac_ls( Args args ){
       StringBuilder sb = new StringBuilder() ;
        for (Map.Entry<String, Integer> entry : _userStatistics
                .entrySet()) {
            sb.append(entry.getKey()).
                    append("  ->  ").
                    append(entry.getValue()).
                    append("\n");
        }
       return sb.toString();
    }
    private void updateStatistics( String userName ){
        Integer count = _userStatistics.get(userName);
        int c = count == null ? 0 : count;
        _userStatistics.put( userName , c + 1) ;
        _requestCount ++ ;
    }
    /**
      * and of course the interface definition
      */
    @Override
    public synchronized Map<String,String> getUserMetaData( String userName , String userRole , List<String> attributes )

         throws Exception {

       //
       // 'attributes' is a list of keys somebody (door)
       // needs from us. We are assumed to prepare
       // a map containing the 'key' and the
       // corresponding values.
       // we should at least be prepared to know the
       // 'uid','gid' of the user.
       // If we are not sure about the user, we should
       // throw an exception rather returning an empty
       // map.
       //
       updateStatistics( userName ) ;
       //
       // get the information for the user
       //
       File userData = new File( _baseDir , userName ) ;
       if( ! userData.exists() ) {
           throw new
                   IllegalArgumentException(
                   _ourName + " : user not found : " + userName);
       }

       BufferedReader br = new BufferedReader( new FileReader( userData ) ) ;
       Map<String, String> result = CollectionFactory.newHashMap();
       //
       // load the hash from file
       //
       try{
           String line;
           while( ( line = br.readLine() ) != null ){
              StringTokenizer st = new StringTokenizer(line,"=");
              try{
                 result.put( st.nextToken() , st.nextToken() ) ;
              }catch(Exception ee ){

              }
           }
       }finally{

           try{ br.close() ; }catch(Exception ee){}
       }
       //
       // check for minimum requirments
       //
       if( ( result.get("uid") == null ) ||
           ( result.get("gid") == null ) ||
           ( result.get("home") == null )  ) {
           throw new
                   IllegalArgumentException(
                   _ourName + " : insufficient info from user : " + userName);
       }

       //
       // prepare the result
       // (as a matter of fact, it would be ok to simply
       //  return 'result'.
       //
       Map<String, String> answer = CollectionFactory.newHashMap();
        for (Object attribute : attributes) {
            String key = (String) attribute;
            String value = result.get(key);
            if (value != null) {
                answer.put(key, value);
            }
        }

       return answer ;

   }
   public String toString(){
      return "rc="+_requestCount;
   }


}
