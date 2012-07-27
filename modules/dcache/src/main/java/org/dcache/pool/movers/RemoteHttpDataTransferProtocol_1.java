package org.dcache.pool.movers;

/**
 * @author Patrick F.
 * @author Timur Perelmutov. timur@fnal.gov
 * @version 0.0, 28 Jun 2002
 */

import diskCacheV111.vehicles.*;
import diskCacheV111.util.Base64;
import diskCacheV111.util.PnfsId;
import diskCacheV111.util.CacheException;
import org.dcache.pool.repository.Allocator;

import dmg.cells.nucleus.*;
import java.io.*;
import java.net.URL;
import java.net.URLConnection;
import java.net.HttpURLConnection;

import java.nio.ByteBuffer;
import org.dcache.pool.repository.RepositoryChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RemoteHttpDataTransferProtocol_1 implements MoverProtocol
{
    private final static Logger _log =
        LoggerFactory.getLogger(RemoteHttpDataTransferProtocol_1.class);
    private final static Logger _logSpaceAllocation = LoggerFactory.getLogger("logger.dev.org.dcache.poolspacemonitor." + RemoteHttpDataTransferProtocol_1.class.getName());

    public static final int READ   =  1;
    public static final int WRITE  =  2;
    public static final long SERVER_LIFE_SPAN= 60 * 5 * 1000; /* 5 minutes */
    private static final int INC_SPACE  =  (50*1024*1024);
    private long    allocated_space;

    private long last_transfer_time    = System.currentTimeMillis();
    private final CellEndpoint   cell;
    private RemoteHttpDataTransferProtocolInfo remoteHttpProtocolInfo;
    private long starttime;
    private URL remoteURL;
    private volatile long transfered;
    private boolean changed;

    public RemoteHttpDataTransferProtocol_1(CellEndpoint cell)
    {
        this.cell = cell;
        say("RemoteHTTPDataTransferAgent_1 created");
    }

    private void say(String str){
        _log.info(str);
    }

    @Override
    public void runIO(RepositoryChannel fileChannel,
                       ProtocolInfo protocol,
                       StorageInfo  storage,
                       PnfsId       pnfsId ,
                       Allocator    allocator,
                       IoMode       access)
        throws Exception
    {
        say("runIO()\n\tprotocol="+
            protocol+",\n\tStorageInfo="+storage+",\n\tPnfsId="+pnfsId+
            ",\n\taccess ="+ access);
        if(! (protocol instanceof RemoteHttpDataTransferProtocolInfo))
            {
                throw new  CacheException(
                                          "protocol info is not RemoteHttpDataTransferProtocolInfo");
            }
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
            String userPassEncoding = Base64.byteArrayToBase64(userInfo.getBytes());
    		httpconnection.setRequestProperty("Authorization", "Basic " +
                                                  userPassEncoding);
            }

        if( access == IoMode.WRITE)
            {
                httpconnection.setDoInput(true);
                httpconnection.setDoOutput(false);
                InputStream httpinput = httpconnection.getInputStream();
                byte[] buffer = new byte[remoteHttpProtocolInfo.getBufferSize()];
                ByteBuffer bb = ByteBuffer.wrap(buffer);
                int read;
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
                        bb.limit(read);
                        fileChannel.write(bb);
                        changed = true;
                        transfered +=read;
                        bb.clear();
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
    @Override
    public long getLastTransferred()
    {
        return last_transfer_time;
    }

    @Override
    public long getBytesTransferred()
    {
        return  transfered;
    }

    @Override
    public long getTransferTime()
    {
        return System.currentTimeMillis() -starttime;
    }

    @Override
    public boolean wasChanged()
    {
        return changed;
    }
}



