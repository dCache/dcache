// $Id: CacheInfo.java,v 1.7 2004-11-05 12:07:18 tigran Exp $

package diskCacheV111.vehicles;

import java.util.*;
import java.io.*;
import diskCacheV111.util.*;

public class CacheInfo implements java.io.Serializable {
    //
    //The level in PNFS dedicated to the cache system
    //
    private static final int __level = 2 ; 
    private CacheStatistics  _cacheStatistics;
    private Hashtable        _cacheLocations;
    private CacheFlags       _cacheFlags  = null ;
    
    private static final long serialVersionUID = -7449372587084685726L;
    
    public class CacheFlags {
        private HashMap      _hash      = new HashMap() ;
        private StringBuffer _inputLine = new StringBuffer() ;
        private void addLine( String line ){
           _inputLine.append(line);
        }
        private void commit(){
           StringTokenizer st = new StringTokenizer(_inputLine.toString(),";") ;
           while( st.hasMoreTokens() ){
              String t = st.nextToken() ;
              int l = t.length() ;
              if( l == 0 )continue ;
              int i = t.indexOf('=');
              if( ( i < 0 ) || ( i == (l-1) ) ){
                 _hash.put( t , "" ) ;
              }else if( i > 0 ){
                 _hash.put( t.substring(0,i) , t.substring(i+1) ) ;
              }
           }
        }
        public String get( String key ){ return (String)_hash.get(key) ; }
        public String remove( String key ){ return (String)_hash.remove(key) ; }
        public void   put( String key , String value ){ _hash.put(key,value);}
        public Set entrySet(){ return _hash.entrySet() ; }
        private String toPnfsString(){
           Iterator i = _hash.entrySet().iterator() ;
           StringBuffer sb = new StringBuffer() ;
           int l = 0 ;
           sb.append(":");
           while( i.hasNext() ){
              Map.Entry entry = (Map.Entry)i.next() ;
              sb.append(entry.getKey()).append("=").
                 append(entry.getValue()).append(";");
              if( ( sb.length() - l ) > 70 ){
                 l = sb.length() ;
                 sb.append("\n:");
              }
           }
           return sb.toString() ;
        }
        public String  toString(){ return toPnfsString() ; }
    
    }
    public CacheFlags getFlags(){ return _cacheFlags ; }
    private void readCacheInfo(BufferedReader file) throws IOException{
        //
	// First line is cache statistics
        //
	_cacheLocations = new Hashtable();
        _cacheFlags     = new CacheFlags() ;
        
	String line      = file.readLine();
        
        if( line == null ){
           _cacheStatistics =  new CacheStatistics() ;
           return ;
        }
        
        
        _cacheStatistics = new CacheStatistics(line) ;
           
	while( ( line = file.readLine() ) != null){
           if( line.length() == 0 )continue ;
           if( line.charAt(0) == ':' ){
              if( line.length() > 1 )_cacheFlags.addLine( line.substring(1) ) ;
           }else{
              _cacheLocations.put(line,"");
           }
        }
        _cacheFlags.commit() ;
    }

    public void writeCacheInfo(PnfsFile pnfsFile) throws IOException{
	File        f  = pnfsFile.getLevelFile(__level);
        
        //
        // currently we accept 1 and 2 but we only write 2.
        //
        _cacheStatistics.setVersion(2);
        
	PrintWriter pw = new PrintWriter(new FileWriter(f));

	try {	   
        
	    pw.println( _cacheStatistics.toPnfsString() );
            pw.println( _cacheFlags.toPnfsString() ) ; 
	    
	    Enumeration e = _cacheLocations.keys();
	    while (e.hasMoreElements()){
		pw.println((String)e.nextElement());
	    }
            
	}finally {
	    pw.close();
	}
    }
    
    public void addCacheLocation(String location){
	_cacheLocations.put(location,"");
    }

    public boolean clearCacheLocation(String location){
	//Returns true if location was actually in the list
	return _cacheLocations.remove(location) != null;
    }

    public Hashtable getCacheLocations(){
	return _cacheLocations;
    }

    public CacheStatistics getCacheStatistics(){
	return _cacheStatistics;
    }

    public String toString(){
        StringBuffer sb = new StringBuffer() ;
	sb.append(_cacheStatistics.toString())  ;	
	Enumeration e   = _cacheLocations.keys();
	while (e.hasMoreElements())
	    sb.append(" ").append(e.nextElement().toString());
	
	return sb.toString();
    }
	

    //XXX should the update method move from CacheStatistics to here?
    public void setCacheStatistics(CacheStatistics cs){
	_cacheStatistics = cs;
    }

    public CacheInfo(PnfsFile pnfsFile) throws IOException {
	BufferedReader br = 
             new BufferedReader(
                  new FileReader(
                        pnfsFile.getLevelFile(__level)));
	try{
	    readCacheInfo(br);
	}finally{
	    br.close();
	}	    
    }
}


	
    
