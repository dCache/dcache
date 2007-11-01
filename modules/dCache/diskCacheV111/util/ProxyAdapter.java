/*
 * $Id: ProxyAdapter.java,v 1.1.2.2 2007-10-08 07:57:32 behrmann Exp $
 * 
 * $Log: not supported by cvs2svn $
 * Revision 1.1.2.1  2007/05/22 23:23:31  podstvkv
 * Initial version of ActiveAdapter. The common interface ProxyAdapter is introduced for the ActiveAdapter and passive SocketAdapter.
 *
 */

package diskCacheV111.util;

import java.io.IOException;
import java.net.Socket;

public interface ProxyAdapter {

    public void setMaxStreams(int n);

    public void setModeE(boolean modeE);

    public int getClientListenerPort();

    public Socket acceptOnClientListener() throws IOException;

    public int getPoolListenerPort();

    public void setDirClientToPool();

    public void setDirPoolToClient();

    public void close();

    public boolean isFailed();

    public boolean isDebug();

    public void setDebug(boolean debug);

    public boolean isAlive();

    public void join() throws InterruptedException;

    public void join(long millis) throws InterruptedException;

    public void start();

}