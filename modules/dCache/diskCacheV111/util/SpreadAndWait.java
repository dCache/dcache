// $Id: SpreadAndWait.java,v 1.5 2001-08-08 21:27:40 cvs Exp $
package diskCacheV111.util ;
import  java.util.* ;
import  dmg.util.* ;
import  dmg.cells.nucleus.* ;
public class SpreadAndWait implements CellMessageAnswerable  {

   private int         _pending = 0 ;
   private CellNucleus _nucleus = null ;
   private ArrayList   _replies = new ArrayList() ;
   private long        _timeout = 0 ;
   public SpreadAndWait( CellNucleus nucleus , long timeout ){
   
      _nucleus = nucleus ;
      _timeout = timeout ;
   }
   public synchronized void send( CellMessage msg ) throws Exception {
       _nucleus.sendMessage( msg , true , true , this , _timeout ) ;
       _pending++ ;
   }
   public synchronized void waitForReplies() throws InterruptedException {
       while( _pending > 0 )wait() ;
   }
   public synchronized void answerArrived(CellMessage request, 
                                          CellMessage answer){
      _pending-- ;
      _replies.add( answer ) ;
      notifyAll() ;
   }
   public synchronized void exceptionArrived(CellMessage request, 
                                             Exception exception){
      _pending-- ;
      notifyAll() ;
   }	
   public synchronized void answerTimedOut(CellMessage request){
      _pending-- ;
      notifyAll() ;
   }
   public Iterator getReplies(){     return _replies.iterator() ; }
   public int      getReplyCount(){  return _replies.size() ; }
   public List     getReplyList(){ return _replies ; }
   public synchronized CellMessage next() throws InterruptedException {
      //
      //      pending     replies       what
      //         yes        == 0       wait
      //         yes         > 0       return elementAt(0) 
      //         no         == 0       null
      //         no          > 0       return elementAt(0)
      //
      while( ( _pending > 0 ) && ( _replies.size() == 0 ) )wait() ;
      
      if( ( _pending == 0 ) && ( _replies.size() == 0 ) )return null  ;
      
//      CellMessage msg = (CellMessage)_replies.get(0) ;
//      _replies.remove(0) ;
      
      return (CellMessage)_replies.remove(0) ;
   }
}
