// $Id: PoolQueryRepositoryMsg.java,v 1.2 2004-11-05 12:07:20 tigran Exp $

package diskCacheV111.vehicles;

import  diskCacheV111.util.IteratorCookie ;
import  java.util.List ;
public class PoolQueryRepositoryMsg extends PoolMessage {

   private IteratorCookie _cookie  = new IteratorCookie() ;
   private List           _pnfsids = null ;
   
   private static final long serialVersionUID = -7160190467703974689L;
   
   public PoolQueryRepositoryMsg(String poolName ){
      super(poolName) ;
   }
   public PoolQueryRepositoryMsg( String poolName , IteratorCookie cookie ){
      this(poolName);
      _cookie = cookie ;
   }
   public void setReply( IteratorCookie cookie , List pnfsids ){
      _cookie  = cookie ;
      _pnfsids = pnfsids ;
      setReply();
   }
   public List getPnfsIds(){ return _pnfsids ; }
   public IteratorCookie getCookie(){ return _cookie ; }

}
