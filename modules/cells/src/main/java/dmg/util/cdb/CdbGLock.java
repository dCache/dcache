package dmg.util.cdb ;

import java.util.Hashtable;
import java.util.Vector;

public class CdbGLock implements CdbLockListener, CdbLockable {
   private static class LockEntry {
      private static class LockEntryDesc {
          private boolean _isWriteLock;
          private int     _counter;
          private LockEntryDesc( boolean writeLock ){
               _isWriteLock = writeLock ;
               _counter     = 1 ;
          }
          public String toString(){
             return "WriteLock="+_isWriteLock+
                    ";Counter="+_counter ;
          }
      }
      private Thread         _thread;
      private int          _position = -1 ;
      private LockEntryDesc [] _desc = new LockEntryDesc[2] ;

      private LockEntry( Thread thread , boolean writeLock ){
         _thread   = thread ;
         _desc[0]  = new LockEntryDesc(writeLock);
         _position = 0 ;
      }
      public Thread  getThread(){ return _thread ; }
      public boolean isWriteLocked(){
            return _desc[_position]._isWriteLock ;
      }
      public void increment(){ _desc[_position]._counter++ ; }
      public int  getCounter(){ return _desc[_position]._counter  ; }
      public void upgrade() throws CdbLockException {
         if( _position > 0 ) {
             throw new CdbLockException("PANIC close=(_position==1)");
         }
         //
         // prepare the new lock entry
         //
         _desc[++_position] = new LockEntryDesc(true);
      }

      public int degrade() throws CdbLockException {
         if( _position < 0 ) {
             throw new CdbLockException("PANIC close=(_position<0)");
         }
         if( _desc[_position]._counter <= 0 ) {
             throw new CdbLockException("PANIC close=(_counter<=0)");
         }

         _desc[_position]._counter-- ;
         if( _desc[_position]._counter <= 0 ){
            _position-- ;
            return  _position < 0 ? NOTHING_LEFT : WRITE_TO_READ ;
         }else {
             return NOTHING_CHANGED;
         }

      }
      public String toString(){
         StringBuilder sb = new StringBuilder() ;
         sb.append(" +Thread : ").append(_thread).append('\n');
         if( _position < 0 ) {
             sb.append("Not assigned ???\n");
         } else{
            for( int i = 0 ; i < 2 ; i ++ ){
               if( _desc[i] == null ){
                   sb.append("  Desc[").append(i).append("]=null\n");
               }else{
                   sb.append( (_position==i)?"*":" ") ;
                   sb.append(" Desc[").append(i).append("]=")
                           .append(_desc[i]).append('\n');
               }
            }
         }
         return sb.toString() ;
      }
   }
   private static final int   NOTHING_CHANGED  = 0 ;
   private static final int   WRITE_TO_READ    = 1 ;
   private static final int   NOTHING_LEFT     = 2 ;


   private Vector<LockEntry> _list = new Vector<>(8) ;
   private Hashtable<Thread, LockEntry> _hash = new Hashtable<>() ;
   private CdbLockListener   _listener;
   private CdbLockable       _creator;
   public CdbGLock( CdbLockListener listener ){
     _listener = listener ;
   }
   public CdbGLock(){ _listener = this ; }
   public CdbGLock( CdbLockable creator ){
      _listener = this ;
      _creator  = creator ;
   }
   public  String toString(){
      StringBuilder sb = new StringBuilder() ;
      for( int i = 0  ; i < _list.size() ; i++ ){
         sb.append(_list.elementAt(i)) ;
      }
      return sb.toString() ;
   }
   @Override
   public synchronized void close( int flags ) throws CdbLockException {
//      System.out.println( "Asking for close : "+flags ) ;
      Thread    ourThread = Thread.currentThread() ;
      LockEntry entry     = _hash.get( ourThread );
      if( entry == null ) {
          throw new CdbLockException("mutex not owned");
      }
      //
      // decrement the usage count and check if we are still in use.
      //
      boolean wasWriteLocked = entry.isWriteLocked() ;
      switch( entry.degrade() ){
         case WRITE_TO_READ :
            if( ( flags & CdbLockable.COMMIT ) > 0 ) {
                writeLockReleased();
            } else {
                writeLockAborted();
            }
         break ;
         case NOTHING_LEFT :
            _list.removeElement( entry ) ;
            _hash.remove( ourThread ) ;
            if( wasWriteLocked ){
               if( ( flags & CdbLockable.COMMIT ) > 0 ) {
                   writeLockReleased();
               } else {
                   writeLockAborted();
               }
            }else{
               readLockReleased() ;
            }
         break ;
      }
      notifyAll() ;
      if( _creator != null ) {
          _creator.close(CdbLockable.COMMIT);
      }
   }
   @Override
   public synchronized void open( int flags )
         throws CdbLockException,
                InterruptedException              {
//      System.out.println( "Asking for lock : "+flags ) ;
      //
      //  make sure we are holding the container mutex
      //
      if( _creator != null ) {
          _creator.open(CdbLockable.READ);
      }
      //
      // are we already in the thread list
      //
      Thread ourThread = Thread.currentThread() ;
      LockEntry entry = _hash.get( ourThread );
      if( entry != null ){
         //
         // ok we got some kind of lock ( which one ? ) ;
         //
         if( ( ( flags & CdbLockable.WRITE ) > 0 ) &&
             ! entry.isWriteLocked()                 ){
            //
            // upgrade the entry
            //
            entry.upgrade() ;
            //
            // remove the entry from the read list and add to
            // the write waiting list.
            //
            _list.removeElement( entry ) ;
            _list.addElement( entry ) ;
            notifyAll() ;
            //
            // and now wait until we reached the bottom of the queue.
            //
            while(true){
               if( _list.elementAt(0) == entry ) {
                   break;
               }
               wait() ;
            }
            writeLockGranted() ;
            return ;
         }
         //
         // increment the lock thread counter
         //
         entry.increment() ;
         return ;
      }
      //
      // create a new thread lock entry and insert it
      //
      entry = new LockEntry(ourThread,
                            (flags & CdbLockable.WRITE) > 0);

      _list.addElement( entry );
      //
      // we need to destingueck between read and write locks
      // because we only allow one writer or many readers.
      //
      if( ( flags & CdbLockable.WRITE ) > 0 ){
         //////////////////////////////////////////////////////////
         //                                                      //
         //                  The writer                          //
         //                                                      //
         if( ( flags & CdbLockable.NON_BLOCKING ) > 0 ){
            if( _list.elementAt(0) != entry ){
               _list.removeElementAt(_list.size() - 1 ) ;
               throw new CdbLockException("Lock not granted") ;
            }

         }
         while(true){
            if( _list.elementAt(0) == entry ) {
                break;
            }
            wait() ;
         }

         writeLockGranted() ;
      }else{
         //////////////////////////////////////////////////////////
         //                                                      //
         //                  The reader                          //
         //                                                      //
         if( ( flags & CdbLockable.NON_BLOCKING ) > 0 ){
            int i;
            for( i = 0 ;
                 ( i < _list.size() ) &&
                 ( ! (_list.elementAt(i)).isWriteLocked() ) &&
                 ( _list.elementAt(i) != entry ) ;
                 i++ ) {
            }
            if( i == _list.size() ) {
                throw new CdbLockException("Panic : 1");
            }
            if( _list.elementAt(i) != entry ){
               _list.removeElementAt(_list.size() - 1 ) ;
               throw new CdbLockException("Lock not granted") ;
            }

         }
         while(true){
            int i;
            for( i = 0 ;
                 ( i < _list.size() ) &&
                 ( ! (_list.elementAt(i)).isWriteLocked() ) &&
                 ( _list.elementAt(i) != entry ) ;
                 i++ ) {
            }
            if( i == _list.size() ) {
                throw new CdbLockException("Panic : 2");
            }
            if( _list.elementAt(i) == entry ) {
                break;
            }
            wait() ;
         }
         readLockGranted() ;

      }
      _hash.put( ourThread , entry ) ;

   }

   @Override
   public void readLockGranted() {}
   @Override
   public void writeLockGranted(){}
   @Override
   public void readLockReleased(){}
   @Override
   public void writeLockReleased(){}
   @Override
   public void writeLockAborted() {}

   public static void main( String [] args ) throws Exception {
       CdbGLock lock = new CdbGLock() ;
       long start , opened , finished ;
       for( int i = 0 ; i < 3 ; i++ ){
          start = System.currentTimeMillis() ;
          lock.open( CdbLockable.WRITE ) ;
          opened = System.currentTimeMillis() ;
          lock.close(CdbLockable.COMMIT) ;
          finished = System.currentTimeMillis() ;
          System.out.println( "Open   : "+(opened-start) ) ;
          System.out.println( "Close  : "+(finished-opened) ) ;
       }

   }

}
