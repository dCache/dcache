// $Id: PoolCheckMessage.java,v 1.2 2004-11-05 12:07:19 tigran Exp $

package diskCacheV111.vehicles;
import java.util.*; 


public class PoolCheckMessage 
       extends PoolMessage 
       implements PoolCheckable {

    private Map _map = null ;
    
    private static final long serialVersionUID = 2300324248952047438L;
    
    public PoolCheckMessage( String poolName ){
       super(poolName) ;
    }
    public void setTagMap( Map map ){ _map = map ; }
    public Map  getTagMap(){ return _map ; }
    public String toString(){
      StringBuffer sb = new StringBuffer() ;
      sb.append(super.toString()).append(";");
      if( _map != null ){
          sb.append("tags={");
          Iterator it = _map.entrySet().iterator() ;
          while( it.hasNext() ){
             Map.Entry entry = (Map.Entry)it.next() ;
             sb.append((String)entry.getKey()).
                append("=").
                append((String)entry.getValue()).
                append(";"); 
          }
          sb.append("};");

      }else{
          sb.append("NOTAGS;");
      }
      return sb.toString();
    }
}
