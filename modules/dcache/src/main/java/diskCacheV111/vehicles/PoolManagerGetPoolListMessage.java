// $Id: PoolManagerGetPoolListMessage.java,v 1.2 2004-11-05 12:07:19 tigran Exp $

package diskCacheV111.vehicles ;

import java.util.ArrayList;
import java.util.List;

public class PoolManagerGetPoolListMessage extends PoolManagerMessage {

   private List<String> _poolList;

   private static final long serialVersionUID = 5654583135549534321L;

   public PoolManagerGetPoolListMessage(){
      super(true);
   }

   public void setPoolList( List<String> list ){
      _poolList = new ArrayList<>(list);
   }

   public List<String> getPoolList(){ return _poolList ; }


}

