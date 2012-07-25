package diskCacheV111.admin ;

import java.util.* ;
import java.io.* ;

import dmg.cells.nucleus.* ;
import dmg.util.* ;

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
    public String hh_ls = "" ;
    public String ac_ls( Args args ){
       StringBuilder sb = new StringBuilder() ;
       Iterator i = _userStatistics.entrySet().iterator() ;
       while( i.hasNext() ){
          Map.Entry entry = (Map.Entry)i.next() ;
          sb.append(entry.getKey().toString()).
             append("  ->  ").
             append(entry.getValue().toString()).
             append("\n") ;
       }
       return sb.toString();
    }
    private void updateStatistics( String userName ){
        Integer count = (Integer)_userStatistics.get(userName);
        int c = count == null ? 0 : count;
        _userStatistics.put( userName , c + 1) ;
        _requestCount ++ ;
    }
    /**
      * and of course the interface definition
      */
    @Override
    public synchronized Map getUserMetaData( String userName , String userRole , List attributes )

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
           String line = null ;
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
       Iterator it = attributes.iterator() ;
       while( it.hasNext() ){
          String key   = (String)it.next() ;
          String value = (String)result.get(key) ;
          if( value != null ) {
              answer.put(key, value);
          }
       }

       return answer ;

   }
   public String toString(){
      return "rc="+_requestCount;
   }


}
