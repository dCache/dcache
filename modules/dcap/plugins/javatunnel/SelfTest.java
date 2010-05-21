/*
 * $Id: SelfTest.java,v 1.4 2006-10-11 09:50:12 tigran Exp $
 */

package javatunnel;

import          java.net.*;
import          java.io.*;

import javax.net.ServerSocketFactory;

class SelfTest {

    public SelfTest(boolean isServer, int port, boolean isClient, String serverHost) {

        System.out.println("New SelfTest server=" + isServer + " port=" + port + " client=" + isClient + " host=" + serverHost);

        try {
            if(isServer) {
                Reciver r = new Reciver(port);
                r.start();
            }
            if( isClient ) {
                Sender s = new Sender(serverHost, port);
                s.start();
            }
        } catch(Exception e) {
            System.out.println(e);
        }
    }

    public static void main(String[] args) {

        boolean isServer = true;
        boolean isClient = true;
        int port = 1717;
        String host = "localhost";

        Args arg = new Args(args);

        if( arg.getOpt("port") != null ) {
            port =  Integer.parseInt( arg.getOpt("port") );
        }

        if( arg.getOpt("server") != null ) {
            isClient = false;
        }

        if( arg.getOpt("client") != null ) {
            isClient = true;
        }

        if( arg.getOpt("host") != null ) {
            host = arg.getOpt("host");
        }


         new SelfTest(isServer, port, isClient, host);
         //new GssTunnel("ftp/dcache0.desy.de@DESY.DE");
    }

    class Sender extends Thread {

        private String _host;
        private int _port;
        Sender(String host, int port) {
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
                s = new TunnelSocket( _host, _port, new GssTunnel("tigran@DESY.DE", "ftp/dcache0.desy.de@DESY.DE"));
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


    class Reciver extends Thread {

        private int _port;
        Reciver(int port) {
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

                String[] initArgs = {"javatunnel.GsiTunnel", "dummy"};

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
