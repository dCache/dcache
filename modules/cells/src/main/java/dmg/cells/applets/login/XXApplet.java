package dmg.cells.applets.login ;

import java.applet.*;

public class      XXApplet
       extends    Applet
       implements Runnable              {


    private static final long serialVersionUID = -6137469238934008771L;
    private Thread _sleep ;
    private Thread _inter ;
    @Override
    public void init(){
         _sleep = new Thread( this ) ;
         _inter = new Thread( this ) ;
         _sleep.start() ;
         _inter.start() ;

    }
    private final Object _lock = new Object() ;
    @Override
    public void run() {

       Thread current = Thread.currentThread() ;

       if( current == _sleep ){
          System.out.println( "Going to sleep" ) ;
          try{
              synchronized(_lock){ _lock.wait(10000) ; }
              System.out.println( "Returning from sleep" ) ;
          }catch( InterruptedException ie ){
              System.out.println( "System sleep interrupted" ) ;
          }
       }else if( current == _inter ){
           try{ current.sleep( 5000 ) ; }catch(InterruptedException e){}
           System.out.println( "killing .. " ) ;
           synchronized( _lock){ _lock.notifyAll() ; }
           System.out.println( "killing done" ) ;


       }

    }


    public static void main( String [] args ){
        Applet a = new XXApplet() ;
        a.init() ;
    }



}
