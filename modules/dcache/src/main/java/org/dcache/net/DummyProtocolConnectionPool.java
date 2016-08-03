package org.dcache.net;

import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import java.nio.channels.SocketChannel;


public class DummyProtocolConnectionPool  implements ChallengeReader {


	@Override
        public Object getChallenge(SocketChannel socketChannel) {

		Object challenge = null;

		try {
            Socket socket = socketChannel.socket();

			InputStream is  = socket.getInputStream();
            byte[] buf = new byte[64];
			int b;
			int i = 0;
			while(i<buf.length) {

				b = is.read();
				if( b < 0 ) {
					throw new IOException ("bad challenge");
				}

				if(b == '\n' || b == '\r' ) {
                                    break;
                                }
				buf[i++] = (byte)b;

			}

			challenge = new String(buf, 0, i);
			System.out.println("New Challenge: " + challenge);

		}catch(Exception e) {

		}

		return challenge;
	}



	public static void main(String[] args) {


		try {


			DummyProtocolConnectionPool dp = new DummyProtocolConnectionPool();
			ProtocolConnectionPoolFactory cf = new ProtocolConnectionPoolFactory(8998, dp);

			ProtocolConnectionPool pcp = cf.getConnectionPool(0);
			int i = 0;
			while(true) {

				System.out.println("Requesing socket by challenge "+Integer.toString(i)  );
				Socket s = (pcp.getSocket(Integer.toString(i))).socket();
				System.out.println("Got it!");
				s.close();
				++i;
			}

		}catch(Exception e) {
			e.printStackTrace();
		}

	}


}




