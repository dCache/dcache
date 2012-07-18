package dmg.util ;

public class BufferScheduler {

   private BufferDescriptor [] _fullArray ;
   private BufferDescriptor [] _emptyArray ;
   private BufferDescriptor [] _all ;
   
   private int _nob ;
   private int _bufferSize ;
   
   private Object _waitForFullLock  = new Object()  ;
   private Object _waitForEmptyLock = new Object() ;
   
   private int _nextToPut  = 0 ;
   private int _nextToGet  = 0 ;
   
   private int _nextEmpty  = 0 ;
   
   public BufferScheduler( int nob , int bufferSize ){
     _nob        = nob ;
     _bufferSize = bufferSize ;
     _fullArray  = new BufferDescriptor[_nob] ;
     _emptyArray = new BufferDescriptor[_nob] ;
     _all        = new BufferDescriptor[_nob] ;
     for( int i = 0 ; i  < _nob ; i++ ) {
         _emptyArray[i] = _all[i] = new BufferDescriptor(_bufferSize, i);
     }
     
     _nextEmpty = _nob - 1 ;
          
   }
   public String toString(){
     StringBuilder sb = new StringBuilder() ;
     for( int i = 0 ; i < _nob ; i++ ) {
         sb.append("").append(_all[i]).append("\n");
     }
     return sb.toString() ;

   }
   public BufferDescriptor getFullBuffer() throws InterruptedException {
      BufferDescriptor b = getFromFilled() ;
      b.setMode( BufferDescriptor.DRAINING ) ;
      return b ;
   }
   public BufferDescriptor getEmptyBuffer() throws InterruptedException {
      BufferDescriptor b = getFromEmpty() ;
      b.setMode( BufferDescriptor.FILLING ) ;
      return b ;
   }
   public void release( BufferDescriptor b ){
      int mode = b.getMode() ;
      switch( mode ){
        case  BufferDescriptor.FILLING :
           b.setMode( BufferDescriptor.FILLED ) ;
           putToFilled( b ) ; 
        break ;    
        case  BufferDescriptor.DRAINING :
           b.setMode( BufferDescriptor.EMPTY ) ;
           putToEmpty( b ) ; 
        break ;
        default : 
          throw new IllegalArgumentException(
             "Invalid Mode for release : "+b.modeToString() ) ;
      }
   }
   private void putToFilled( BufferDescriptor b ){
      synchronized( _waitForFullLock ){
         _fullArray[(_nextToPut++)%_nob] = b ;
         if( ( _nextToPut - _nextToGet ) == 1 ) {
             _waitForFullLock.notifyAll();
         }
      }
   } 
   private BufferDescriptor getFromFilled() throws InterruptedException{
      synchronized( _waitForFullLock ){
         while( ( _nextToPut - _nextToGet ) == 0 ) {
             _waitForFullLock.wait();
         }
         return _fullArray[(_nextToGet++)%_nob] ;
      }
   }
   private BufferDescriptor getFromEmpty() throws InterruptedException {
     synchronized( _waitForEmptyLock ){
        while( _nextEmpty < 0 ) {
            _waitForEmptyLock.wait();
        }
        return _emptyArray[_nextEmpty--] ;
     }
   }
   private void putToEmpty( BufferDescriptor b ){
      synchronized( _waitForEmptyLock ){
         _emptyArray[++_nextEmpty] = b ;
         if( _nextEmpty == 0 ) {
             _waitForEmptyLock.notifyAll();
         }
      }
   }

}
