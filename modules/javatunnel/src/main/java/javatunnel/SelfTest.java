/*
 * $Id: SelfTest.java,v 1.4 2006-10-11 09:50:12 tigran Exp $
 */

package javatunnel;

import org.dcache.dss.KerberosDssContextFactory;

import javax.net.ServerSocketFactory;

import java.io.DataInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;

import dmg.protocols.telnet.TunnelSocket;

class SelfTest {

    public static void main(String[] args)
    {

        int port = 1717;
        String host = "localhost";

        Reciever r = new Reciever(port);
        r.start();

        Sender s = new Sender(host, port);
        s.start();
    }

    private static class Sender extends Thread {

        private String _host;
        private int _port;
        Sender(String host, int port) {
            super("Sender");
            _port = port;
            _host = host;
        }

        @Override
        public void     run() {
            OutputStream out;
            InputStream in;
            Socket s;
            PrintStream os;
            DataInputStream is;

            try {
                s = new DssSocket( _host, _port, new KerberosDssContextFactory("tigran@DESY.DE", "nfs/anahit.desy.de@DESY.DE"));
                Thread.sleep(20000);
                out  = s.getOutputStream();
                in  =  s.getInputStream();

                os = new PrintStream(out);
                is = new DataInputStream(in);
            } catch(Exception e) {
                System.out.println(e);
                return;
            }

            while (!Thread.interrupted()) {
                try {

                    System.out.println(" Sender to reciver : " +  "Hello tunnel" );
                    os.println("Hello tunnel");

                    System.out.println(" Sender Got : " +  is.readLine() );

                    Thread.sleep(2000);
                }  catch(Exception e) {
                    System.out.println(e);
                    e.printStackTrace();
                    break;
                }

            }
        }
    }


    private static class Reciever extends Thread {

        private int _port;
        Reciever(int port) {
            super("Reciever");
            _port = port;
        }

        @Override
        public void     run() {

            ServerSocket server;
            Socket s;
            OutputStream out;
            InputStream in;
            PrintStream os;
            DataInputStream is;
            try {

                String[] initArgs = {"javatunnel.GssTunnel", "nfs/anahit.desy.de@DESY.DE"};

                ServerSocketFactory factory = new DssServerSocketCreator(initArgs);

                server = factory.createServerSocket();
                server.bind(new InetSocketAddress( _port ) );
                s = server.accept();
                ((TunnelSocket) s).verify();

                DssSocket ts = (DssSocket)s;
                System.out.println( ts.getSubject() );

                   out  = s.getOutputStream();
                   in  =  s.getInputStream();
                os = new PrintStream(out);
                is = new DataInputStream(in);

            } catch(Throwable e) {
                System.out.println(e);
                e.printStackTrace();
                return;
            }
            while (!Thread.interrupted()) {
                try {


                    System.out.println(" RECIVER Got : " +  is.readLine());

                    System.out.println(" RECIVER to sender : " +  "What?!" );
                    os.println("What?!");

                } catch(Exception e) {
                    System.out.println(e);
                    e.printStackTrace();
                    break;
                }

            }
        }
    }

}
