package org.dcache.pool.movers;

/**
 * @author Patrick F.
 * @author Timur Perelmutov. timur@fnal.gov
 * @version 0.0, 28 Jun 2002
 */

import diskCacheV111.vehicles.*;
import diskCacheV111.util.PnfsId;
import diskCacheV111.util.PnfsFile;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.HttpConnectionHandler;
import org.dcache.pool.repository.Allocator;

import dmg.cells.nucleus.*;
import java.io.*;
import java.net.URL;
import java.net.URLConnection;
import java.net.HttpURLConnection;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.InetAddress;
import java.util.Hashtable;
import java.util.StringTokenizer;

import org.apache.log4j.Logger;

public class RemoteHttpDataTransferProtocol_1 implements MoverProtocol
{
    private final static Logger _log =
        Logger.getLogger(RemoteHttpDataTransferProtocol_1.class);
    private final static Logger _logSpaceAllocation = Logger.getLogger("logger.dev.org.dcache.poolspacemonitor." + RemoteHttpDataTransferProtocol_1.class.getName());

    public static final int READ   =  1;
    public static final int WRITE  =  2;
    public static final long SERVER_LIFE_SPAN= 60 * 5 * 1000; /* 5 minutes */
    private static final int INC_SPACE  =  (50*1024*1024);
    private long    allocated_space  = 0;

    private long last_transfer_time    = System.currentTimeMillis();
    private final CellEndpoint   cell;
    private RemoteHttpDataTransferProtocolInfo remoteHttpProtocolInfo;
    private CellPath pathToSource;
    private ServerSocket httpserver;
    private long starttime;
    private RandomAccessFile diskFile;
    private long timeout_time;
    private URL remoteURL;
    private volatile long transfered  = 0;
    private boolean changed;

    public RemoteHttpDataTransferProtocol_1(CellEndpoint cell)
    {
        this.cell = cell;
        say("RemoteHTTPDataTransferAgent_1 created");
    }

    private void say(String str){
        _log.info(str);
    }

    private void esay(String str){
        _log.error(str);
    }

    public void runIO(RandomAccessFile diskFile,
                       ProtocolInfo protocol,
                       StorageInfo  storage,
                       PnfsId       pnfsId ,
                       Allocator    allocator,
                       int          access)
        throws Exception
    {
        say("runIO()\n\tprotocol="+
            protocol+",\n\tStorageInfo="+storage+",\n\tPnfsId="+pnfsId+
            ",\n\taccess ="+(((access & MoverProtocol.WRITE) != 0)?"WRITE":"READ"));
        if(! (protocol instanceof RemoteHttpDataTransferProtocolInfo))
            {
                throw new  CacheException(
                                          "protocol info is not RemoteHttpDataTransferProtocolInfo");
            }
        this.diskFile = diskFile;
        starttime = System.currentTimeMillis();

        remoteHttpProtocolInfo = (RemoteHttpDataTransferProtocolInfo) protocol;

        remoteURL = new URL(remoteHttpProtocolInfo.getSourceHttpUrl());
        URLConnection connection = remoteURL.openConnection();
        if(! (connection instanceof HttpURLConnection))
            {
                throw new CacheException("wrong URL connection type");

            }
        HttpURLConnection httpconnection = (HttpURLConnection) connection;

        String userInfo = remoteURL.getUserInfo();
        if (userInfo != null)
            {
		// set the authentication
                String userPassEncoding = new sun.misc.BASE64Encoder().encode(userInfo.getBytes());
    		httpconnection.setRequestProperty("Authorization", "Basic " +
                                                  userPassEncoding);
            }

        if((access & MoverProtocol.WRITE) != 0)
            {
                httpconnection.setDoInput(true);
                httpconnection.setDoOutput(false);
                InputStream httpinput = httpconnection.getInputStream();
                byte[] buffer = new byte[remoteHttpProtocolInfo.getBufferSize()];
                int read = 0;
                _logSpaceAllocation.debug("ALLOC: " + pnfsId + " : " + INC_SPACE);
                allocator.allocate(INC_SPACE);
                allocated_space+=INC_SPACE;

                while((read = httpinput.read(buffer)) != -1)
                    {
                        last_transfer_time    = System.currentTimeMillis();
                        if(transfered+read > allocated_space)
                            {
                                _logSpaceAllocation.debug("ALLOC: " + pnfsId + " : " + INC_SPACE);
                                allocator.allocate(INC_SPACE);
                                allocated_space+=INC_SPACE;

                            }
                        diskFile.write(buffer,0,read);
                        changed = true;
                        transfered +=read;
                    }

                say("runIO() wrote "+transfered+"bytes");
            }
        else
            {
                httpconnection.setDoInput(false);
                httpconnection.setDoOutput(true);
                OutputStream httpoutput = httpconnection.getOutputStream();
            }
        say(" runIO() done");
    }
    public long getLastTransferred()
    {
        return last_transfer_time;
    }

    private synchronized void setTimeoutTime(long t)
    {
        timeout_time = t;
    }
    private synchronized long  getTimeoutTime()
    {
        return timeout_time;
    }
    public void setAttribute(String name, Object attribute)
    {
    }
    public Object getAttribute(String name)
    {
        return null;
    }
    public long getBytesTransferred()
    {
        return  transfered;
    }

    public long getTransferTime()
    {
        return System.currentTimeMillis() -starttime;
    }

    public boolean wasChanged()
    {
        return changed;
    }
}



