package org.dcache.pool.movers;

/**
 * @author Patrick F.
 * @author Timur Perelmutov. timur@fnal.gov
 * @version 0.0, 28 Jun 2002
 */

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.DCapClientPortAvailableMessage;
import diskCacheV111.vehicles.DCapClientProtocolInfo;
import diskCacheV111.vehicles.ProtocolInfo;
import diskCacheV111.vehicles.StorageInfo;
import diskCacheV111.vehicles.StorageInfos;

import dmg.cells.nucleus.CellEndpoint;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellPath;

import org.dcache.pool.repository.Allocator;
import org.dcache.pool.repository.RepositoryChannel;
import org.dcache.util.NetworkUtils;
import org.dcache.util.PortRange;
import org.dcache.vehicles.FileAttributes;

public class DCapClientProtocol_1 implements MoverProtocol
{
    private static final Logger _log =
        LoggerFactory.getLogger(DCapClientProtocol_1.class);
    public static final int READ   =  1;
    public static final int WRITE  =  2;
    private long last_transfer_time    = System.currentTimeMillis();
    private final CellEndpoint   cell;
    private DCapClientProtocolInfo dcapClient;
    private long starttime;
    private volatile long transfered;


    public DCapClientProtocol_1(CellEndpoint cell)
    {
        this.cell = cell;
        say("DCapClientProtocol_1 created");
    }

    private void say(String str){
        _log.info(str);
    }

    private void esay(String str){
        _log.error(str);
    }

    private void esay(Throwable t)
    {
        _log.error(t.toString());
    }

    @Override
    public void runIO(FileAttributes fileAttributes,
                       RepositoryChannel fileChannel,
                       ProtocolInfo protocol ,
                       Allocator    allocator ,
                       IoMode         access) throws CacheException, IOException, InterruptedException
    {
        PnfsId pnfsId = fileAttributes.getPnfsId();
        StorageInfo storage = StorageInfos.extractFrom(fileAttributes);
        say("runIO()\n\tprotocol="+
            protocol+",\n\tStorageInfo="+storage+",\n\tPnfsId="+pnfsId+
            ",\n\taccess ="+access);
        if(! (protocol instanceof DCapClientProtocolInfo))
            {
                throw new  CacheException(
                                          "protocol info is not RemoteGsiftpransferProtocolInfo");
            }
        starttime = System.currentTimeMillis();

        dcapClient = (DCapClientProtocolInfo) protocol;


        CellPath cellpath = new CellPath(dcapClient.getInitiatorCellName(),
                                         dcapClient.getInitiatorCellDomain());
        say(" runIO() RemoteGsiftpTranferManager cellpath="+cellpath);

        ServerSocket serverSocket;
        int port;
        try {
            String dcachePorts = System.getProperty("org.dcache.net.tcp.portrange");
            PortRange portRange = (dcachePorts != null) ? PortRange.valueOf(dcachePorts) : new PortRange(0);
            serverSocket = new ServerSocket();
            port = portRange.bind(serverSocket, 1);
        } catch(IOException ioe) {
            esay("exception while trying to create a server socket : "+ioe);
            throw ioe;
        }

        InetAddress localAddress = NetworkUtils.getLocalAddress(dcapClient.getSocketAddress().getAddress());

        DCapClientPortAvailableMessage cred_request =
            new DCapClientPortAvailableMessage(localAddress.getCanonicalHostName(),
            port,dcapClient.getId());


        say(" runIO() created message");
        cell.sendMessage (new CellMessage(cellpath,cred_request));
        say("waiting for dcap server connection");
        Socket dcap_socket = serverSocket.accept();
        say("connected");
        try
            {
                serverSocket.close();
            }
        catch(IOException ioe)
            {
                esay("failed to close server socket");
                esay(ioe);
                // we still can continue, this is non-fatal
            }



        if( access == IoMode.WRITE)
            {
                dcapReadFile(dcap_socket,fileChannel,allocator);
            }
        else
            {
                throw new IOException("read is not implemented");
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

    private void dcapReadFile(Socket _socket,
                              RepositoryChannel fileChannel,
                              Allocator allocator) throws IOException, InterruptedException
    {
        last_transfer_time    = System.currentTimeMillis();
        DataInputStream in   = new DataInputStream(_socket.getInputStream());
        DataOutputStream out = new DataOutputStream(_socket.getOutputStream());

        say("<init>");
        int _sessionId = in.readInt();


        int challengeSize = in.readInt();
        while (challengeSize > 0) {
            challengeSize -= in.skipBytes(challengeSize);
        }

        say("<gettingFilesize>");
        out.writeInt(4); // bytes following
        out.writeInt(9);  // locate command
        //
        // waiting for reply
        //
        int following = in.readInt();
        if(following < 28) {
            throw new
                    IOException("Protocol Violation : ack too small : " + following);
        }

        int type = in.readInt();
        if(type != 6)   // REQUEST_ACK
        {
            throw new
                    IOException("Protocol Violation : NOT REQUEST_ACK : " + type);
        }

        int mode = in.readInt();
        if(mode != 9) // SEEK
        {
            throw new
                    IOException("Protocol Violation : NOT SEEK : " + mode);
        }

        int returnCode = in.readInt();
        if(returnCode != 0){
            String error = in.readUTF();
            throw new
                IOException("Seek Request Failed : ("+
                             returnCode+") "+error);
        }
        long filesize = in.readLong();
        say("<WaitingForSpace-"+filesize+">");
        allocator.allocate(filesize);
        //
        in.readLong();   // file position


        say("<StartingIO>");
        //
        // request the full file
        //
        out.writeInt(12); // bytes following
        out.writeInt(2);  // read command
        out.writeLong(filesize);
        //
        // waiting for reply
        //
        following = in.readInt();
        if(following < 12) {
            throw new
                    IOException("Protocol Violation : ack too small : " + following);
        }

        type = in.readInt();
        if(type != 6)   // REQUEST_ACK
        {
            throw new
                    IOException("Protocol Violation : NOT REQUEST_ACK : " + type);
        }

        mode = in.readInt();
        if(mode != 2) // READ
        {
            throw new
                    IOException("Protocol Violation : NOT SEEK : " + mode);
        }

        returnCode = in.readInt();
        if(returnCode != 0){
            String error = in.readUTF();
            throw new
                IOException("Read Request Failed : ("+
                             returnCode+") "+error);
        }
        say("<RunningIO>");
        //
        // expecting data chain
        //
        //
        // waiting for reply
        //
        following = in.readInt();
        if(following < 4) {
            throw new
                    IOException("Protocol Violation : ack too small : " + following);
        }

        type = in.readInt();
        if(type != 8)   // DATA
        {
            throw new
                    IOException("Protocol Violation : NOT DATA : " + type);
        }

        byte [] data = new byte[256*1024];
        ByteBuffer bb = ByteBuffer.wrap(data);
        int nextPacket;
        while(true){
            if((nextPacket = in.readInt()) < 0) {
                break;
            }

            int restPacket = nextPacket;

            while(restPacket > 0){
                bb.clear();
                int block = Math.min(restPacket , data.length);
                //
                // we collect a full block before we write it out
                // (a block always fits into our buffer)
                //
                int position = 0;
                for(int rest = block;  rest > 0;){
                    int rc = in.read(data , position , rest);
                    last_transfer_time    = System.currentTimeMillis();

                    if(rc < 0) {
                        throw new
                                IOException("Premature EOF");
                    }

                    rest     -= rc;
                    position += rc;
                }
                transfered +=block;
                bb.limit(block);
                fileChannel.write(bb);

                restPacket -= block;
            }
        }
        say("<WaitingForReadAck>");
        //
        // waiting for reply
        //
        following = in.readInt();
        if(following < 12) {
            throw new
                    IOException("Protocol Violation : ack too small : " + following);
        }

        type = in.readInt();
        if(type != 7)   // REQUEST_FIN
        {
            throw new
                    IOException("Protocol Violation : NOT REQUEST_ACK : " + type);
        }

        mode = in.readInt();
        if(mode != 2) // READ
        {
            throw new
                    IOException("Protocol Violation : NOT SEEK : " + mode);
        }

        returnCode = in.readInt();
        if(returnCode != 0){
            String error = in.readUTF();
            throw new
                IOException("Read Fin Failed : ("+
                             returnCode+") "+error);
        }
        say("<WaitingForCloseAck>");
        //
        out.writeInt(4);  // bytes following
        out.writeInt(4);  // close request
        //
        // waiting for reply
        //
        following = in.readInt();
        if(following < 12) {
            throw new
                    IOException("Protocol Violation : ack too small : " + following);
        }

        type = in.readInt();
        if(type != 6)   // REQUEST_FIN
        {
            throw new
                    IOException("Protocol Violation : NOT REQUEST_ACK : " + type);
        }

        mode = in.readInt();
        if(mode != 4) // READ
        {
            throw new
                    IOException("Protocol Violation : NOT SEEK : " + mode);
        }

        returnCode = in.readInt();
        if(returnCode != 0){
            String error = in.readUTF();
            throw new
                IOException("Close ack Failed : ("+
                             returnCode+") "+error);
        }
        say("<Done>");
    }
}
