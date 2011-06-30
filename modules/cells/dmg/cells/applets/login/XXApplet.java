package dmg.cells.applets.login ;

import java.applet.*;
import java.awt.* ;
import java.awt.event.* ;
import java.net.* ;
import java.io.*;
import dmg.util.* ;
import dmg.protocols.ssh.* ;

public class      XXApplet
       extends    Applet
       implements Runnable              {



    private Thread _sleep ;
    private Thread _inter ;
    public void init(){
         _sleep = new Thread( this ) ;
         _inter = new Thread( this ) ;
         _sleep.start() ;
         _inter.start() ;

    }
    private Object _lock = new Object() ;
    public void run() {

       Thread current = Thread.currentThread() ;

       if( current == _sleep ){
          System.out.println( "Going to sleep" ) ;
          try{
              synchronized(_lock){ _lock.wait(10000) ; }
              System.out.println( "Returning from sleep" ) ;
          }catch( Exception ie ){
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
