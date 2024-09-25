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
import org.dcache.nfs.nfsstat;
import org.dcache.nfs.status.DelayException;
import org.dcache.nfs.v4.ClientSession;
import org.dcache.nfs.v4.CompoundBuilder;
import org.dcache.nfs.v4.xdr.COMPOUND4args;
import org.dcache.nfs.v4.xdr.COMPOUND4res;
import org.dcache.nfs.v4.xdr.READ4resok;
import org.dcache.nfs.v4.xdr.SEQUENCE4args;
import org.dcache.nfs.v4.xdr.WRITE4resok;
import org.dcache.nfs.v4.xdr.clientid4;
import org.dcache.nfs.v4.xdr.nfs4_prot;
import org.dcache.nfs.v4.xdr.nfs_argop4;
import org.dcache.nfs.v4.xdr.nfs_fh4;
import org.dcache.nfs.v4.xdr.nfs_opnum4;
import org.dcache.nfs.v4.xdr.sequenceid4;
import org.dcache.nfs.v4.xdr.slotid4;
import org.dcache.nfs.v4.xdr.state_protect_how4;
import org.dcache.nfs.v4.xdr.stateid4;
import org.dcache.nfs.vfs.Inode;
import org.dcache.nfs.vfs.VirtualFileSystem;
import org.dcache.oncrpc4j.rpc.IoStrategy;
import org.dcache.oncrpc4j.rpc.OncRpcException;
import org.dcache.oncrpc4j.rpc.OncRpcSvc;
import org.dcache.oncrpc4j.rpc.OncRpcSvcBuilder;
import org.dcache.oncrpc4j.rpc.RpcAuth;
import org.dcache.oncrpc4j.rpc.RpcAuthTypeUnix;
import org.dcache.oncrpc4j.rpc.RpcCall;
import org.dcache.oncrpc4j.rpc.RpcTransport;
import org.dcache.oncrpc4j.rpc.net.IpProtocolType;
import org.dcache.util.NetworkUtils;

/**
 * A {@link ProxyIoAdapter} which proxies requests to another NFSv4.1 server.
 */
public class NfsProxyIo implements ProxyIoAdapter {

    private static final int ROOT_UID = 0;
    private static final int ROOT_GID = 0;
    private static final int[] ROOT_GIDS = new int[0];

    private static final String IMPL_DOMAIN = "dCache.ORG";
    private static final String IMPL_NAME = "proxyio-nfs-client";

    /**
     * How long we wait for an IO request. The typical NFS client will wait 30 sec. We will use a
     * shorter timeout to avoid retry.
     */
    private static final int IO_TIMEOUT = 15;
    private static final TimeUnit IO_TIMEOUT_UNIT = TimeUnit.SECONDS;

    /**
     * Most up-to-date seqid for a given stateid as defined by rfc5661.
     */
    private static final int SEQ_UP_TO_DATE = 0;

    private clientid4 _clientIdByServer;
    private sequenceid4 _sequenceID;

    private final stateid4 stateid;
    private final nfs_fh4 fh;

    private final InetSocketAddress remoteClient;
    private final RpcCall client;
    private final OncRpcSvc rpcsvc;
    private final RpcTransport transport;
    private final ScheduledExecutorService sessionThread;

    private ClientSession clientSession;

    public NfsProxyIo(InetSocketAddress poolAddress, InetSocketAddress remoteClient, Inode inode,
          stateid4 stateid, long timeout, TimeUnit timeUnit) throws IOException {
        this.remoteClient = remoteClient;
        rpcsvc = new OncRpcSvcBuilder()
              .withClientMode()
              .withPort(0)
              .withIpProtocolType(IpProtocolType.TCP)
              .withIoStrategy(IoStrategy.SAME_THREAD)
              .withServiceName("proxy-io-rpc-selector-" + poolAddress.getAddress().getHostAddress())
              .withSelectorThreadPoolSize(1)
              .build();

        try {
            rpcsvc.start();
            transport = rpcsvc.connect(poolAddress, timeout, timeUnit);
        } catch (IOException | Error | RuntimeException e) {
            rpcsvc.stop();
            throw e;
        }

        RpcAuth credential = new RpcAuthTypeUnix(ROOT_UID, ROOT_GID, ROOT_GIDS,
              (int) (System.currentTimeMillis() / 1000),
              NetworkUtils.getCanonicalHostName());
        client = new RpcCall(nfs4_prot.NFS4_PROGRAM, nfs4_prot.NFS_V4, credential, transport);
        sessionThread = Executors.newSingleThreadScheduledExecutor(
              new ThreadFactoryBuilder()
                    .setNameFormat(
                          "proxy-nfs-session-" + poolAddress.getAddress().getHostAddress() + "-%d")
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
              .withPutfh(fh)
              .withRead(dst.remaining(), position, stateid)
              .withTag("pNFS read")
              .build();
        COMPOUND4res compound4res = sendCompoundInSession(args);
        READ4resok res = compound4res.resarray.get(2).opread.resok4;
        dst.put(res.data);
        return new ReadResult(needToRead - dst.remaining(), res.eof);
    }

    @Override
    public synchronized VirtualFileSystem.WriteResult write(ByteBuffer src, long position)
          throws IOException {

        COMPOUND4args args = new CompoundBuilder()
              .withPutfh(fh)
              .withWrite(position, src, stateid)
              .withTag("pNFS write")
              .build();

        COMPOUND4res compound4res = sendCompoundInSession(args);
        WRITE4resok res = compound4res.resarray.get(2).opwrite.resok4;
        return new VirtualFileSystem.WriteResult(
              VirtualFileSystem.StabilityLevel.fromStableHow(res.committed), res.count.value);
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
            destroy_clientid();
        } finally {
            rpcsvc.stop();
        }
    }

    /**
     * Call remote procedure nfsProcCompound.
     *
     * @param arg parameter (of type COMPOUND4args) to the remote procedure call.
     * @return Result from remote procedure call (of type COMPOUND4res).
     * @throws OncRpcException if an ONC/RPC error occurs.
     * @throws IOException     if an I/O error occurs.
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

    public RpcTransport getTransport() {
        return client.getTransport();
    }

    private COMPOUND4res sendCompound(COMPOUND4args compound4args) throws IOException {

        COMPOUND4res compound4res;
        /*
         * TODO: escape if it takes too long
         */
        do {
            compound4res = nfsProcCompound(compound4args);
        } while (canRetry(compound4res.status));

        nfsstat.throwIfNeeded(compound4res.status);
        return compound4res;
    }

    private COMPOUND4res sendCompoundInSession(COMPOUND4args compound4args)
          throws OncRpcException, IOException {

        if (compound4args.argarray[0].argop == nfs_opnum4.OP_SEQUENCE) {
            throw new IllegalArgumentException("The operation sequence should not be included");
        }

        nfs_argop4[] extendedOps = new nfs_argop4[compound4args.argarray.length + 1];
        System.arraycopy(compound4args.argarray, 0, extendedOps, 1, compound4args.argarray.length);
        compound4args.argarray = extendedOps;

        var slot = clientSession.acquireSlot();
        try {

            COMPOUND4res compound4res;
            /*
             * TODO: escape if it takes too long
             */
            do {
                nfs_argop4 op = new nfs_argop4();
                op.argop = nfs_opnum4.OP_SEQUENCE;
                op.opsequence = new SEQUENCE4args();
                op.opsequence.sa_cachethis = false;

                op.opsequence.sa_slotid = slot.getId();
                op.opsequence.sa_highest_slotid = new slotid4(clientSession.maxRequests() - 1);
                op.opsequence.sa_sequenceid = slot.nextSequenceId();
                op.opsequence.sa_sessionid = clientSession.sessionId();

                compound4args.argarray[0] = op;

                compound4res = nfsProcCompound(compound4args);

            } while (canRetry(compound4res.status));

            nfsstat.throwIfNeeded(compound4res.status);
            return compound4res;
        } finally {
            clientSession.releaseSlot(slot);
        }
    }

    private boolean canRetry(int status) {
        // as we do proxy, the only expected retryable error is DELAY
        return status ==  nfsstat.NFSERR_DELAY;
    }

    private synchronized void exchange_id() throws OncRpcException, IOException {

        COMPOUND4args args = new CompoundBuilder()
              .withExchangeId(IMPL_DOMAIN, IMPL_NAME, UUID.randomUUID().toString(), 0,
                    state_protect_how4.SP4_NONE)
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

        var createSesssionRep = compound4res.resarray.get(0).opcreate_session.csr_resok4;
        var sessionid = createSesssionRep.csr_sessionid;
        int maxRequests = createSesssionRep.csr_fore_chan_attrs.ca_maxrequests.value;

        _sequenceID.value = 0;

        clientSession = new ClientSession(sessionid, maxRequests);

        sessionThread.scheduleAtFixedRate(() -> {
                  try {
                      this.sequence();
                  } catch (IOException ex) {
                      //
                  }
              },
              60, 60, TimeUnit.SECONDS);
    }

    private synchronized void sequence() throws OncRpcException, IOException {

        COMPOUND4args args = new CompoundBuilder()
              .withTag("sequence")
              .build();
        COMPOUND4res compound4res = sendCompoundInSession(args);
    }

    private synchronized void destroy_session() throws OncRpcException, IOException {

        COMPOUND4args args = new CompoundBuilder()
              .withDestroysession(clientSession.sessionId())
              .withTag("destroy_session")
              .build();

        COMPOUND4res compound4res = sendCompound(args);
    }

    private void destroy_clientid() throws OncRpcException, IOException {

        COMPOUND4args args = new CompoundBuilder()
              .withDestroyclientid(_clientIdByServer)
              .withTag("destroy_clientid")
              .build();
        @SuppressWarnings("unused")
        COMPOUND4res compound4res = sendCompound(args);
    }
}
