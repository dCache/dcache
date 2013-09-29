package org.dcache.pool.movers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLConnection;
import java.nio.ByteBuffer;

import diskCacheV111.util.Base64;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.ProtocolInfo;
import diskCacheV111.vehicles.RemoteHttpDataTransferProtocolInfo;
import diskCacheV111.vehicles.StorageInfo;

import dmg.cells.nucleus.CellEndpoint;

import org.dcache.pool.repository.Allocator;
import org.dcache.pool.repository.RepositoryChannel;
import org.dcache.vehicles.FileAttributes;

public class RemoteHttpDataTransferProtocol_1 implements MoverProtocol
{
    private final static Logger _log =
        LoggerFactory.getLogger(RemoteHttpDataTransferProtocol_1.class);

    private static final int INC_SPACE = (50 * 1024 * 1024);

    private long allocated_space;
    private long last_transfer_time = System.currentTimeMillis();
    private long starttime;
    private volatile long transferred ;

    public RemoteHttpDataTransferProtocol_1(CellEndpoint cell)
    {
    }

    @Override
    public void runIO(FileAttributes fileAttributes,
                      RepositoryChannel fileChannel,
                      ProtocolInfo protocol,
                      Allocator    allocator,
                      IoMode       access)
        throws CacheException, IOException, InterruptedException
    {
        PnfsId pnfsId = fileAttributes.getPnfsId();
        StorageInfo storage = fileAttributes.getStorageInfo();
        _log.info("Active HTTP: Protocol={}, StorageInfo={}, PnfsId={}, Access={}",
                protocol, storage, pnfsId, access);
        if (!(protocol instanceof RemoteHttpDataTransferProtocolInfo)) {
            throw new CacheException("protocol info is not RemoteHttpDataTransferProtocolInfo");
        }

        starttime = System.currentTimeMillis();
        RemoteHttpDataTransferProtocolInfo remoteHttpProtocolInfo =
            (RemoteHttpDataTransferProtocolInfo) protocol;
        URL remoteURL = new URL(remoteHttpProtocolInfo.getSourceHttpUrl());
        URLConnection connection = remoteURL.openConnection();
        if (!(connection instanceof HttpURLConnection)) {
            throw new CacheException("URL is not usable with active HTTP mover: " + remoteURL);
        }
        HttpURLConnection httpconnection = (HttpURLConnection) connection;

        String userInfo = remoteURL.getUserInfo();
        if (userInfo != null) {
            // set the authentication
            String userPassEncoding = Base64.byteArrayToBase64(userInfo.getBytes());
            httpconnection.setRequestProperty("Authorization", "Basic " + userPassEncoding);
        }

        if (access == IoMode.WRITE) {
            httpconnection.setDoInput(true);
            httpconnection.setDoOutput(false);
            InputStream httpinput = httpconnection.getInputStream();
            byte[] buffer = new byte[remoteHttpProtocolInfo.getBufferSize()];
            ByteBuffer bb = ByteBuffer.wrap(buffer);
                int read;
            allocator.allocate(INC_SPACE);
            allocated_space += INC_SPACE;

            while ((read = httpinput.read(buffer)) != -1) {
                last_transfer_time = System.currentTimeMillis();
                if (transferred+read > allocated_space) {
                    allocator.allocate(INC_SPACE);
                    allocated_space+=INC_SPACE;
                }
                bb.limit(read);
                fileChannel.write(bb);
                transferred +=read;
                bb.clear();
            }
        } else {
            httpconnection.setDoInput(false);
            httpconnection.setDoOutput(true);
            OutputStream httpoutput = httpconnection.getOutputStream();
            throw new UnsupportedOperationException("srmCopy upload not implemented for HTTP");
            // TODO: Implement push
        }
    }

    @Override
    public long getLastTransferred()
    {
        return last_transfer_time;
    }

    @Override
    public long getBytesTransferred()
    {
        return transferred;
    }

    @Override
    public long getTransferTime()
    {
        return System.currentTimeMillis() - starttime;
    }

}



