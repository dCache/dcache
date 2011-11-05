/*
 * $Id: SelfTest.java,v 1.4 2006-10-11 09:50:12 tigran Exp $
 */

package javatunnel;

import          java.net.*;
import          java.io.*;

import javax.net.ServerSocketFactory;

class SelfTest {

    public static void main(String[] args) throws Exception {

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
            OutputStream out  = null;
            InputStream in = null;
            Socket s;
            PrintStream os = null;
            DataInputStream is = null;

            try {
                s = new TunnelSocket( _host, _port, new GssTunnel("tigran@DESY.DE", "nfs/anahit.desy.de@DESY.DE"));
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

            ServerSocket server = null;
            Socket s = null;
            OutputStream out  = null;
            InputStream in = null;
            PrintStream os = null;
            DataInputStream is = null;
            try {

                String[] initArgs = {"javatunnel.GssTunnel", "nfs/anahit.desy.de@DESY.DE"};

                ServerSocketFactory factory = new TunnelServerSocketCreator(initArgs);

                server = factory.createServerSocket();

//                server = new TunnelServerSocket(new GsiTunnel("dummy"));
                server.bind(new InetSocketAddress( _port ) );
                s = server.accept();


               // Convertable tunnel = new TunnelConverter();
                TunnelSocket ts = (TunnelSocket)s;
                ts.verify();
                System.out.println( ts.getSubject() );

                   out  = s.getOutputStream();
                   in  =  s.getInputStream();
                os = new PrintStream(out);
                is = new DataInputStream(in);

            } catch(Exception e) {
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

};
