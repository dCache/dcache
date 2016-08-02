// $Id: PoolCheckMessage.java,v 1.2 2004-11-05 12:07:19 tigran Exp $

package diskCacheV111.vehicles;

import java.util.Map;


public class PoolCheckMessage
       extends PoolMessage
       implements PoolCheckable {

    private Map<String, String> _map;

    private static final long serialVersionUID = 2300324248952047438L;

    public PoolCheckMessage( String poolName ){
       super(poolName) ;
    }

    public String toString(){
      StringBuilder sb = new StringBuilder() ;
      sb.append(super.toString()).append(';');
      if( _map != null ){
          sb.append("tags={");
          for( Map.Entry<String, String> entry : _map.entrySet()){
             sb.append(entry.getKey()).
                append('=').
                append(entry.getValue()).
                append(';');
          }
          sb.append("};");

      }else{
          sb.append("NOTAGS;");
      }
      return sb.toString();
    }
}
