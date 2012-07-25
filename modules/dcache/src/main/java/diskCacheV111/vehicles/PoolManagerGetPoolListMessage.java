// $Id: PoolManagerGetPoolListMessage.java,v 1.2 2004-11-05 12:07:19 tigran Exp $

package diskCacheV111.vehicles ;

import  java.util.* ;

public class PoolManagerGetPoolListMessage extends PoolManagerMessage {

   private List _poolList;

   private static final long serialVersionUID = 5654583135549534321L;

   public PoolManagerGetPoolListMessage(){
      super(true);
   }

   public void setPoolList( List list ){
      _poolList = new ArrayList(list);
   }

   public List getPoolList(){ return _poolList ; }


}

