package org.dcache.net;

import java.nio.channels.SocketChannel;

public interface ChallengeReader {

	Object getChallenge(SocketChannel socketChannel) ;

}
