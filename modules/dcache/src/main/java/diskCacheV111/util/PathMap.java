// $Id: PathMap.java,v 1.5 2007-05-24 13:51:06 tigran Exp $
package diskCacheV111.util ;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.StringTokenizer;

public class PathMap {
    private final Map<String, Object> _root = new HashMap<>() ;

    public static class Entry {
       private final String _key ;
       private final String _rest;
       private final Object _node;

       private Entry( String key , String rest , Object node ){
          _key = key ;
          _rest = rest ;
          _node = node ;
       }
       public Object getNode(){ return _node ;}
       public String getRest(){ return _rest ; }
       public String getKey(){ return _key ; }
       public String toString(){
         return "Key="+_key+";rest="+_rest+";node="+_node;
       }
    }
    public PathMap(){}

    public void add( String path , Object node ){
       StringTokenizer st = new StringTokenizer( path , "/" ) ;
       Map<String, Object> current  = _root ;
       Map<String, Object> newLevel;
       int count = st.countTokens() ;
       for( int i = 0 ; i < count ; i++ ){
          String item = st.nextToken() ;
          Object o = current.get( item ) ;
          if( o == null ){
              if( i == ( count - 1 ) ){
                 current.put( item , node ) ;
                 return ;
              }else{
                 current.put( item , newLevel = new HashMap<>() ) ;
                 current = newLevel ;
              }
          }else if( o instanceof Map ){
              if( i == ( count - 1 ) ){
                throw new
                IllegalArgumentException( "Inconsistent path-2");
              }else{
                 current = (Map<String, Object>)o ;
              }
          }else {
              throw new
                      IllegalArgumentException("Inconsistent path");
          }
       }
    }
    public Entry match( String path ){
       StringTokenizer st = new StringTokenizer( path , "/" ) ;
       Map<String, Object> current  = _root ;
       while( st.hasMoreTokens() ){
          String item = st.nextToken() ;
          Object o = current.get( item ) ;
          if( o == null ){
              throw new
              NoSuchElementException("Path doesn't match");

          }else if( o instanceof Map ){
//              System.out.println("Found : "+item ) ;
              current = (Map<String, Object>)o ;
          }else{
              StringBuilder sb = new StringBuilder() ;
              while( st.hasMoreTokens() ){
                 sb.append("/").append(st.nextToken()) ;
              }
              return new Entry( path , sb.toString(), o ) ;

          }
       }
       Map<String, Object> currentMap = _root ;
       while( true ){
           Iterator<Object> iter = currentMap.values().iterator() ;
           if( ! iter.hasNext() ) {
               throw new
                       IllegalArgumentException("Path to short to match");
           }

           Object o = iter.next() ;

           if( ! ( o instanceof Map ) ) {
               return new Entry(path, "/", o);
           }

           currentMap = (Map<String, Object>)o ;

       }
    }
    public static void main( String [] args ){
       PathMap map = new PathMap() ;
       if( args.length < 2 ){
          System.err.println("Usage : ... <path> <path> ... <match>");
          System.exit(4);
       }
       for( int i = 0 ; i < (args.length-1) ; i++ ) {
           map.add(args[i], args[i]);
       }

       Object o = map.match( args[args.length-1] ) ;
       if( o == null ) {
           System.out.println("Nothing assigned up to here");
       } else {
           System.out.println("-> " + o.toString());
       }
       System.exit(0);
    }
}
