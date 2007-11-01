package dmg.apps.psdl.misc ;

import java.util.* ;
import dmg.apps.psdl.vehicles.StateInfo ;
import dmg.cells.nucleus.CellNucleus ;
import dmg.cells.nucleus.CellPath ;
import dmg.cells.nucleus.CellMessage ;

public class StateInfoUpdater  implements  Runnable {
   private Thread      _worker ;
   private CellNucleus _nucleus ;
   private Hashtable   _pathList = new Hashtable() ;
   private StateInfo   _info ;
   private Object      _lock = new Object() ;
   private int         _period ;
   private String      _name = null ;
   
   public StateInfoUpdater( CellNucleus nucleus , int period ){
      _nucleus = nucleus ;
      _period  = period ;
      _info    = null ;
      _worker  = null ;                        
   }
   public synchronized void addTarget( String path ){
      _pathList.put( path , new CellPath( path ) ) ;
   }
   public synchronized void removeTarget( String pathString ){
      CellPath path = (CellPath)_pathList.remove( pathString ) ;
      if( ( path == null ) || ( _name == null ) )return ;
      try{
         _nucleus.sendMessage( 
             new CellMessage( path , 
                              new StateInfo( _name, false ) 
                            )  ) ;
      }catch(Exception ee ){
//                 System.out.println( "StateInfoUpdater : "+ee ) ;
      }
      
   }
   public String [] getTargets(){
      String [] targets = new String[_pathList.size()] ;
      Enumeration e = _pathList.keys() ;
      for( int i = 0 ; e.hasMoreElements() ; i++ ){
         targets[i] = (String)e.nextElement() ;
      }
      return targets ;
   }
   public void setInfo( StateInfo info ){
      if( info != null ){
           _name = info.getName() ; 
           info.setPeriod( _period ) ;
      }
      synchronized( _lock ){
         
         if( _info == null ){
            _info = info ;
            _worker = new Thread( this ) ;
            _worker.start() ;
         
         }else{
            _info = info ;
         }
         _lock.notifyAll() ;
      }
   }
   public void trigger(){ 
      synchronized( _lock ){
         _lock.notifyAll() ;
      }
   }
   public void run(){
      synchronized( _lock ){
         Enumeration e = null ;
         CellPath path = null ;
         while( true ){
            if( _info == null )return ;
            e = _pathList.elements() ;
            while( e.hasMoreElements() ){
               path = (CellPath)e.nextElement() ;
               try{
                  _nucleus.sendMessage( 
                      new CellMessage( path , _info )  ) ;
               }catch(Exception ee ){
//                 System.out.println( "StateInfoUpdater : "+ee ) ;
               }
            }
            if( ! _info.isUp() ){
                _info = null ;
                return ;
            }
            try{ _lock.wait( _period*1000 ) ; }
            catch( InterruptedException eee){}
         }
      }
   }
   public void down(){ 
      synchronized( _lock ){
         if( _name == null )return ;
         setInfo( new StateInfo( _name, false ) ) ; 
      }
   }
  

}
