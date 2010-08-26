package dmg.util ;

public class Gate {
   private boolean _isOpen = true ;
   public Gate(){}
   public Gate( boolean isOpen ){
      _isOpen = isOpen ;
   }
   public synchronized Object check(){
      while( true ){  
         if( _isOpen )return this ;
         try{ wait() ; }catch( Exception ee ){} ;
      }
   
   }
   public synchronized void open(){ 
     _isOpen = true ;
     notifyAll() ;
   } 
   public synchronized void close(){ 
     _isOpen = false ;
     notifyAll() ;
   } 

}
