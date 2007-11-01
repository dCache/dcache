package diskCacheV111.util ;
import java.util.* ;

public class FsPath {
       
       private ArrayList _list = null ;
       
       public List getPathItemsList()
       {
           if(_list == null) return null;
           return (List) _list.clone();
       }
       
       public FsPath( FsPath path ){
          _list = new ArrayList( path._list ) ;
       }
       
       public FsPath( String path ){ 
          _list = new ArrayList() ;
          add( path ) ; 
       }
       public String toString(){
           return toString(_list);
       }
       
       public static String toString(List pathItemsList)
       {
         if( pathItemsList.size() == 0 )return "/";
         StringBuffer sb = new  StringBuffer() ;
         Iterator i = pathItemsList.iterator() ;
         while( i.hasNext() ){
           sb.append( "/" ).append( i.next().toString() ) ;
         }
         return sb.toString() ;
       }
       
       public void add( String path ){
          if( ( path == null ) || ( path.length() == 0 ) )return ;
          if( path.startsWith("/") )_list.clear() ;
          StringTokenizer st = new StringTokenizer(path,"/");
          while( st.hasMoreTokens() )
             addSingleItem(st.nextToken());
          return ;  
          
       }
       private void addSingleItem( String item ){
          if( item.equals(".") )return ;
          if( item.equals("..") ){
             if( _list.size() > 0 )_list.remove(_list.size()-1);
             return ;
          }
          _list.add(item);
       }
    public static void main( String [] args ){
        FsPath path = new FsPath("/pnfs/desy.de") ;
        System.out.println(path.toString());
        path.add("zeus/users/patrick");
        System.out.println(path.toString());
        path.add("../trude");
        System.out.println(path.toString());
        path.add("/");
        System.out.println(path.toString());
        path.add("pnfs/cern.ch");
        System.out.println(path.toString());
        path.add("./../././");
        System.out.println(path.toString());
    }
}
