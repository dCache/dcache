package dmg.util ;

import java.util.HashMap;
import java.util.Random;

public class PulsSampler {
   private HashMap<Integer, Sample> _map = new HashMap<>() ;
   public class Sample {
      private int _samples   = 20 ;
      private int _deltaBySample;
      private int _delta;
      private int [] _sample;
      private int _currentPosition = -1 ;
      private int _swaps;
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
              for (int element : _sample) {
                  sum += element;
              }
          }
          return " samples="+_samples+";delta="+(_delta/1000)+";sum="+sum+
                 ";result="+getRate() ;
      }
      public int getTicks(){
          int sum = 0 ;
          synchronized( this ){
              for (int element : _sample) {
                  sum += element;
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
      return _map.get( Integer.valueOf(delta) );
   }
   public synchronized void tick(){
      long t = System.currentTimeMillis() ;
       for (Object sample : _map.values()) {
           ((Sample) sample).tick(t);
       }
   }
   public String toString(){
      StringBuilder sb = new StringBuilder() ;
       for (Object sample : _map.values()) {
           sb.append(sample.toString()).append("\n");
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
