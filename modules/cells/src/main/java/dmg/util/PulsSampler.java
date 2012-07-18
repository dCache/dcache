package dmg.util ;

import java.util.* ;

public class PulsSampler {
   private HashMap _map = new HashMap() ;
   public class Sample {
      private int _samples   = 20 ;
      private int _deltaBySample = 0 ;
      private int _delta     = 0 ;
      private int [] _sample = null ;
      private int _currentPosition = -1 ;
      private int _swaps     = 0 ;
      private Sample( int delta , int samples ){
         if( delta < 1 ) {
             throw new
                     IllegalArgumentException("Delta must > 0");
         }
         if( ( samples > 100 ) || ( samples < 10 ) ) {
             throw new
                     IllegalArgumentException("10 <= samples <= 100");
         }
         _samples = samples ;
         _delta   = delta * 1000 ;
         _deltaBySample = _delta / _samples ;
         _sample  = new int[_samples] ;
      }
      public synchronized void tick(){
         tick( System.currentTimeMillis() ) ;
      }
      public synchronized void tick( long c ){
         int position = (int)( ( c / _deltaBySample ) % _samples ) ;
         if( position != _currentPosition ){
             _swaps ++ ;
             _sample[position] = 0 ;
             _currentPosition = position ;
         }
         _sample[position] ++ ;
      }
      public String toString(){
          int sum = 0 ;
          synchronized( this ){
             for( int i = 0 ; i < _sample.length ; i++ ) {
                 sum += _sample[i];
             }
          }
          return " samples="+_samples+";delta="+(_delta/1000)+";sum="+sum+
                 ";result="+getRate() ;
      }
      public int getTicks(){ 
          int sum = 0 ;
          synchronized( this ){
             for( int i = 0 ; i < _sample.length ; i++ ) {
                 sum += _sample[i];
             }
          }
          return sum ;   
      }
      public int getDelta(){ return _delta ; }
      public int getRealDelta(){ 
         return _swaps < _samples ? 
                ( _swaps == 0 ? 1 : _deltaBySample * _swaps ) : 
                _delta  ; 
      }
      public float getRate(){
         return ((float)getTicks())/((float)getRealDelta())*(float)1000.;
      }
   }
   public synchronized Sample newSample( int delta ){
      Sample sample = new Sample( delta , 20 ) ;
      _map.put(delta, sample ) ;
      return sample ;
   }
   public synchronized Sample getSample( int delta ){
      return (Sample)_map.get( Integer.valueOf(delta) ) ;
   }
   public synchronized void tick(){
      long t = System.currentTimeMillis() ;
      Iterator i = _map.values().iterator() ;
      while( i.hasNext() ) {
          ((Sample) (i.next())).tick(t);
      }
      return  ;
   }
   public String toString(){
      StringBuffer sb = new StringBuffer() ;
      Iterator i = _map.values().iterator() ;
      while( i.hasNext() ) {
          sb.append(i.next().toString()).append("\n");
      }
      return  sb.toString() ;
   }
   public static void main( String [] args )
   {
      final PulsSampler sampler = new PulsSampler() ;
      final PulsSampler.Sample [] sample = new PulsSampler.Sample[3] ;
      sample[0] = sampler.newSample(60) ;
      sample[1] = sampler.newSample(5*60) ;
      sample[2] = sampler.newSample(10*60) ;
      new Thread( 
         new Runnable(){
           @Override
           public void run(){
              while(true){
                 System.out.println(sampler.toString() ) ;
                 try{ Thread.sleep(1000) ; }
                 catch(Exception e ){ break ; }
              }
           }
         } 
      ).start() ;
      new Thread( 
         new Runnable(){
           @Override
           public void run(){
              Random random = new Random() ;
              while(true){  
                 int n = random.nextInt() ;
                 n = n > 0 ? n : -n ;
                 long l = (long)(n%100) ;
                 try{ Thread.sleep(l) ; }
                 catch(Exception e ){ break ; }
                 sampler.tick();
              }
           }
         } 
      ).start() ;
      
   }
   public static void mains( String [] args )
   {
      int n = 0 ;
      while(true){
         n++ ;
         if( n < 0 ) {
             System.out.println("Swapped");
         }
      }
   }

}
