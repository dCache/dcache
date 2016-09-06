package org.dcache.chimera.nfsv41.door.proxy;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.dcache.util.NetworkUtils;

import org.dcache.nfs.nfsstat;
import org.dcache.nfs.status.DelayException;
import org.dcache.nfs.v4.client.CompoundBuilder;
import org.dcache.nfs.v4.xdr.COMPOUND4args;
import org.dcache.nfs.v4.xdr.COMPOUND4res;
import org.dcache.nfs.v4.xdr.READ4resok;
import org.dcache.nfs.v4.xdr.WRITE4resok;
import org.dcache.nfs.v4.xdr.clientid4;
import org.dcache.nfs.v4.xdr.nfs_fh4;
import org.dcache.nfs.v4.xdr.nfs4_prot;
import org.dcache.nfs.v4.xdr.nfs_opnum4;
import org.dcache.nfs.v4.xdr.nfs_resop4;
import org.dcache.nfs.v4.xdr.sequenceid4;
import org.dcache.nfs.v4.xdr.sessionid4;
import org.dcache.nfs.v4.xdr.stateid4;
import org.dcache.nfs.v4.xdr.state_protect_how4;
import org.dcache.nfs.vfs.Inode;
import org.dcache.nfs.vfs.VirtualFileSystem;
import org.dcache.xdr.IoStrategy;
import org.dcache.xdr.IpProtocolType;
import org.dcache.xdr.OncRpcException;
import org.dcache.xdr.OncRpcClient;
import org.dcache.xdr.RpcAuth;
import org.dcache.xdr.RpcAuthTypeUnix;
import org.dcache.xdr.RpcCall;
import org.dcache.xdr.XdrTransport;

/**
 * A {@link ProxyIoAdapter} which proxies requests to an other NFSv4.1 server.
 */
public class NfsProxyIo implements ProxyIoAdapter {

    // FIXME: for now we will use only a single slot. e.q serialize all requests
    private static final int SLOT_ID = 0;
    private static final int MAX_SLOT_ID = 0;

    private static final int ROOT_UID = 0;
    private static final int ROOT_GID = 0;
    private static final int[] ROOT_GIDS = new int[0];

    private static final String IMPL_DOMAIN = "dCache.ORG";
    private static final String IMPL_NAME = "proxyio-nfs-client";

    /**
     * How long we wait for an IO request. The typical NFS client will wait
     * 30 sec. We will use a shorter timeout to avoid retry.
     */
    private static final int IO_TIMEOUT = 15;
    private static final TimeUnit IO_TIMEOUT_UNIT = TimeUnit.SECONDS;

    /**
     * Most up-to-date seqid for a given stateid as defined by rfc5661.
     */
    private static final int SEQ_UP_TO_DATE = 0;

    private clientid4 _clientIdByServer;
    private sequenceid4 _sequenceID;
    private sessionid4 _sessionid;

    private final stateid4 stateid;
    private final nfs_fh4 fh;

    private final InetSocketAddress remoteClient;
    private final RpcCall client;
    private final OncRpcClient rpcClient;
    private final XdrTransport transport;
    private final ScheduledExecutorService sessionThread;

    public NfsProxyIo(InetSocketAddress poolAddress,  InetSocketAddress remoteClient, Inode inode, stateid4 stateid, long timeout, TimeUnit timeUnit) throws IOException {
        this.remoteClient = remoteClient;
        rpcClient = new OncRpcClient(poolAddress, IpProtocolType.TCP);
        try {
            transport = rpcClient.connect(timeout, timeUnit);
        } catch (IOException | Error | RuntimeException e) {
            rpcClient.close();
            throw e;
        }

        RpcAuth credential = new RpcAuthTypeUnix(ROOT_UID, ROOT_GID, ROOT_GIDS,
                (int) (System.currentTimeMillis() / 1000),
                NetworkUtils.getCanonicalHostName());
        client = new RpcCall(nfs4_prot.NFS4_PROGRAM, nfs4_prot.NFS_V4, credential, transport);
        sessionThread = Executors.newSingleThreadScheduledExecutor(
                new ThreadFactoryBuilder()
                .setNameFormat("proxy-nfs-session-" + poolAddress.getAddress().getHostAddress() + "-%d")
                .build()
        );

        exchange_id();
        create_session();
        fh = new nfs_fh4(inode.toNfsHandle());
        this.stateid = new stateid4(stateid.other, SEQ_UP_TO_DATE);
    }

    @Override
    public synchronized ReadResult read(ByteBuffer dst, long position) throws IOException {

        int needToRead = dst.remaining();
        COMPOUND4args args = new CompoundBuilder()
                .withSequence(false, _sessionid, _sequenceID.value, SLOT_ID, MAX_SLOT_ID)
                .withPutfh(fh)
                .withRead(dst.remaining(), position, stateid)
                .withTag("pNFS read")
                .build();
        COMPOUND4res compound4res = sendCompound(args);
        READ4resok res = compound4res.resarray.get(2).opread.resok4;
        dst.put(res.data);
        return new ReadResult(needToRead - dst.remaining(), res.eof);
    }

    @Override
    public synchronized VirtualFileSystem.WriteResult write(ByteBuffer src, long position) throws IOException {

        byte[] data = new byte[src.remaining()];
        src.get(data);

        COMPOUND4args args = new CompoundBuilder()
                .withSequence(false, _sessionid, _sequenceID.value, SLOT_ID, MAX_SLOT_ID)
                .withPutfh(fh)
                .withWrite(position, data, stateid)
                .withTag("pNFS write")
                .build();

        COMPOUND4res compound4res = sendCompound(args);
        WRITE4resok res = compound4res.resarray.get(2).opwrite.resok4;
        return new VirtualFileSystem.WriteResult(VirtualFileSystem.StabilityLevel.fromStableHow(res.committed), res.count.value);
    }

    @Override
    public String toString() {
        return String.format("    OS=%s, cl=[%s], pool=[%s]",
                stateid,
                remoteClient.getAddress().getHostAddress(),
                transport.getRemoteSocketAddress().getAddress().getHostAddress());
    }

    @Override
    public stateid4 getStateId() {
        return stateid;
    }

    @Override
    public void close() throws IOException {
        sessionThread.shutdown();
        try {
            destroy_session();
        } finally {
            rpcClient.close();
        }
    }
    /**
     * Call remote procedure nfsProcCompound.
     *
     * @param arg parameter (of type COMPOUND4args) to the remote procedure
     * call.
     * @return Result from remote procedure call (of type COMPOUND4res).
     * @throws OncRpcException if an ONC/RPC error occurs.
     * @throws IOException if an I/O error occurs.
     */
    public COMPOUND4res nfsProcCompound(COMPOUND4args arg)
            throws OncRpcException, IOException {
        COMPOUND4res result = new COMPOUND4res();

        try {
            client.call(nfs4_prot.NFSPROC4_COMPOUND_4, arg, result, IO_TIMEOUT, IO_TIMEOUT_UNIT);
        } catch (TimeoutException e) {
            throw new DelayException(e.getMessage(), e);
        }

        return result;
    }

    public XdrTransport getTransport() {
        return client.getTransport();
    }

    private COMPOUND4res sendCompound(COMPOUND4args compound4args)
            throws OncRpcException, IOException {

        COMPOUND4res compound4res = nfsProcCompound(compound4args);
        processSequence(compound4res);
        nfsstat.throwIfNeeded(compound4res.status);
        return compound4res;
    }

    private synchronized void exchange_id() throws OncRpcException, IOException {

        COMPOUND4args args = new CompoundBuilder()
                .withExchangeId(IMPL_DOMAIN, IMPL_NAME, UUID.randomUUID().toString(), 0, state_protect_how4.SP4_NONE)
                .withTag("exchange_id")
                .build();

        COMPOUND4res compound4res = sendCompound(args);

        _clientIdByServer = compound4res.resarray.get(0).opexchange_id.eir_resok4.eir_clientid;
        _sequenceID = compound4res.resarray.get(0).opexchange_id.eir_resok4.eir_sequenceid;

        if ((compound4res.resarray.get(0).opexchange_id.eir_resok4.eir_flags.value
                & nfs4_prot.EXCHGID4_FLAG_USE_PNFS_DS) == 0) {
            throw new IOException("remote server is not a DS");
        }
    }

    private synchronized void create_session() throws OncRpcException, IOException {

        COMPOUND4args args = new CompoundBuilder()
                .withCreatesession(_clientIdByServer, _sequenceID)
                .withTag("create_session")
                .build();

        COMPOUND4res compound4res = sendCompound(args);

        _sessionid = compound4res.resarray.get(0).opcreate_session.csr_resok4.csr_sessionid;
        _sequenceID.value = 0;

        sessionThread.scheduleAtFixedRate(() -> {
            try {
                this.sequence();
            } catch (IOException ex) {
                //
            }
        },
                60, 60, TimeUnit.SECONDS);
    }

    public void processSequence(COMPOUND4res compound4res) {

        nfs_resop4 res = compound4res.resarray.get(0);
        if (res.resop == nfs_opnum4.OP_SEQUENCE && res.opsequence.sr_status == nfsstat.NFS_OK) {
            ++_sequenceID.value;
        }
    }

    private synchronized void sequence() throws OncRpcException, IOException {

        COMPOUND4args args = new CompoundBuilder()
                .withSequence(false, _sessionid, _sequenceID.value, SLOT_ID, MAX_SLOT_ID)
                .withTag("sequence")
                .build();
        COMPOUND4res compound4res = sendCompound(args);
    }

    private synchronized void destroy_session() throws OncRpcException, IOException {

        COMPOUND4args args = new CompoundBuilder()
                .withDestroysession(_sessionid)
                .withTag("destroy_session")
                .build();

        COMPOUND4res compound4res = sendCompound(args);
    }
}
