package org.dcache.net;

import java.nio.channels.*;

public interface ChallengeReader {

	Object getChallenge(SocketChannel socketChannel) ;

}
