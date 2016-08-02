package dmg.protocols.snmp ;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.SocketException;
import java.util.Iterator;
import java.util.Vector;

/**
  *
  *
  * @author Patrick Fuhrmann
  * @version 0.1, 15 Feb 1998
  */
public class SnmpServer implements Runnable {
    Vector<SnmpEventListener> _actionListener = new Vector<>() ;
    int            _port ;
    DatagramSocket _socket ;
    Thread         _listenThread ;

    public SnmpServer( int port ) throws SocketException {
       _port   = port ;
       _socket = new DatagramSocket( port ) ;
       _listenThread = new Thread( this ) ;
       _listenThread.start();
    }
    public void addSnmpEventListener( SnmpEventListener listener ){
       _actionListener.addElement( listener ) ;
    }
    @Override
    public void run(){
      if( Thread.currentThread() == _listenThread ){

        while(true){
           try{
              byte  []       b = new byte[2048] ;
              DatagramPacket p = new DatagramPacket( b , b.length ) ;

              _socket.receive( p ) ;

              SnmpObject snmp = SnmpObject.generate(
                                    p.getData(),0,
                                    p.getLength());

              SnmpRequest request = new SnmpRequest( snmp ) ;
              SnmpEvent   event   = new SnmpEvent(
                                               p.getAddress() ,
                                               request ) ;

               Iterator<SnmpEventListener> iterator = _actionListener.iterator();
              SnmpEventListener listener ;
               while (iterator.hasNext()) {
                   listener = iterator.next();
                   SnmpRequest answer = listener.snmpEventArrived(event);
                   if (answer == null) {
                       System.out.println("Answer discarded");
                       continue;
                   }
                   byte[] x = answer.getSnmpBytes();
                   DatagramPacket dp = new DatagramPacket(
                           x, x.length,
                           p.getAddress(), p.getPort());
                   System.out.println("Sending " + x.length +
                           " bytes to " + p.getAddress() +
                                      ':' + p.getPort());
                   _socket.send(dp);
               }

           }catch( Exception e ){
              System.out.println( "Error while sending : "+e ) ;
           }


        }
      }
    }

}
