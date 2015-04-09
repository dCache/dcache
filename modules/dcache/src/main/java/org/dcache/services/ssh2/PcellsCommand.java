package org.dcache.services.ssh2;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import org.apache.sshd.server.Command;
import org.apache.sshd.server.Environment;
import org.apache.sshd.server.ExitCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import diskCacheV111.admin.LegacyAdminShell;
import diskCacheV111.services.space.LinkGroup;
import diskCacheV111.services.space.Space;
import diskCacheV111.services.space.message.GetLinkGroupsMessage;
import diskCacheV111.services.space.message.GetSpaceTokensMessage;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.TimeoutCacheException;
import diskCacheV111.vehicles.IoJobInfo;

import dmg.cells.applets.login.DomainObjectFrame;
import dmg.cells.nucleus.CellEndpoint;
import dmg.cells.nucleus.CellPath;
import dmg.cells.nucleus.NoRouteToCellException;
import dmg.util.CommandException;

import org.dcache.cells.CellStub;
import org.dcache.util.Args;
import org.dcache.util.TransferCollector;
import org.dcache.util.TransferCollector.Transfer;

public class PcellsCommand implements Command, Runnable
{
    private static final Logger LOGGER =
            LoggerFactory.getLogger(PcellsCommand.class);

    private final CellEndpoint _endpoint;
    private final CellStub _spaceManager;
    private final CellStub _poolManager;
    private final CellStub _pnfsManager;
    private LegacyAdminShell _shell;
    private InputStream _in;
    private ExitCallback _exitCallback;
    private OutputStream _out;
    private Thread _adminShellThread;
    private ExecutorService _executor = Executors.newCachedThreadPool();
    private ScheduledExecutorService _scheduler = Executors.newSingleThreadScheduledExecutor();
    private volatile boolean _done = false;
    private final TransferCollector _collector;
    private volatile List<Transfer> _transfers = Collections.emptyList();

    public PcellsCommand(CellEndpoint endpoint,
                         CellStub spaceManager,
                         CellStub poolManager,
                         CellStub pnfsManager,
                         TransferCollector transferCollector)
    {
        _endpoint = endpoint;
        _spaceManager = spaceManager;
        _poolManager = poolManager;
        _pnfsManager = pnfsManager;
        _collector = transferCollector;
    }

    @Override
    public void setErrorStream(OutputStream err)
    {
        // we don't use the error stream
    }

    @Override
    public void setExitCallback(ExitCallback callback)
    {
        _exitCallback = callback;
    }

    @Override
    public void setInputStream(InputStream in)
    {
        _in = in;
    }

    @Override
    public void setOutputStream(OutputStream out)
    {
        _out = out;
    }

    @Override
    public void start(Environment env) throws IOException
    {
        String user = env.getEnv().get(Environment.ENV_USER);
        _shell = new LegacyAdminShell(user, _endpoint, "");
        _adminShellThread = new Thread(this);
        _adminShellThread.start();
        _scheduler.schedule(this::updateTransfers, 30, TimeUnit.SECONDS);
    }

    @Override
    public void destroy()
    {
        if (_adminShellThread != null) {
            _adminShellThread.interrupt();
        }
        _executor.shutdownNow();
        _scheduler.shutdownNow();
    }

    @Override
    public void run() {
        try {
            final ObjectInputStream in = new ObjectInputStream(_in);
            final ObjectOutputStream out = new ObjectOutputStream(_out);
            out.flush();
            Object obj;
            while (!_done && (obj = in.readObject()) != null) {
                if (!(obj instanceof DomainObjectFrame)) {
                    LOGGER.error("Received unsupported request type: {}", obj.getClass());
                    continue;
                }

                final DomainObjectFrame frame = (DomainObjectFrame) obj;
                LOGGER.trace("Frame id {} received", frame.getId());
                _executor.execute(new Runnable() {
                    @Override
                    public void run()
                    {
                        Object result;
                        try {
                            if (frame.getDestination() == null) {
                                result = _shell.executeCommand(frame.getPayload().toString());
                            } else {
                                switch (frame.getDestination()) {
                                case "PnfsManager":
                                    result = _shell.executeCommand(_pnfsManager.getDestinationPath(), frame.getPayload());
                                    break;
                                case "PoolManager":
                                    result = _shell.executeCommand(_poolManager.getDestinationPath(), frame.getPayload());
                                    break;
                                case "SrmSpaceManager":
                                    if (frame.getPayload().equals("ls -l")) {
                                        result = listSpaceReservations();
                                    } else {
                                        result = _shell.executeCommand(_spaceManager.getDestinationPath(), frame.getPayload());
                                    }
                                    break;
                                case "TransferObserver":
                                    if (frame.getPayload().equals("ls iolist")) {
                                        result = listTransfers(_transfers);
                                    } else {
                                        result = _shell.executeCommand(new CellPath(frame.getDestination()), frame.getPayload());
                                    }
                                    break;
                                default:
                                    result = _shell.executeCommand(new CellPath(frame.getDestination()), frame.getPayload());
                                    break;
                                }
                            }
                        } catch (CommandException e) {
                            result = e;
                            _done = true;
                            try {
                                in.close();
                            } catch (IOException ignored) {
                            }
                        } catch (TimeoutCacheException e) {
                            if (e.getCause() instanceof NoRouteToCellException) {
                                result = e.getCause();
                            } else {
                                result = null;
                            }
                        } catch (Exception ae) {
                            result = ae;
                        }
                        frame.setPayload(result);
                        try {
                            synchronized (out) {
                                out.writeObject(frame);
                                out.flush();
                                out.reset();  // prevents memory leaks...
                            }
                        } catch (IOException e) {
                            LOGGER.error("Problem sending result : {}", e);
                        }
                    }
                });
            }
        } catch (IOException e) {
            LOGGER.warn("I/O failure in pcells connection: {}", e.toString());
        } catch (ClassNotFoundException e) {
            LOGGER.error("Received unsupported request type: {}", e.getMessage());
        } finally {
            _exitCallback.onExit(0);
        }
    }

    private String listSpaceReservations() throws CacheException, InterruptedException
    {
        /* Query information from space manager. */
        Collection<Space> spaces = _spaceManager.sendAndWait(new GetSpaceTokensMessage()).getSpaceTokenSet();
        Collection<LinkGroup> groups = _spaceManager.sendAndWait(new GetLinkGroupsMessage()).getLinkGroups();

        /* Build pcells compatible list. */
        StringBuilder out = new StringBuilder();

        out.append("Reservations:\n");
        for (Space space : spaces) {
            out.append(space).append('\n');
        }
        out.append("total number of reservations: ").append(spaces.size()).append('\n');
        out.append("total number of bytes reserved: ")
                .append(spaces.stream().mapToLong(Space::getSizeInBytes).sum()).append('\n');

        out.append("\nLinkGroups:\n");
        for (LinkGroup group : groups) {
            out.append(group).append('\n');
        }
        out.append("total number of linkGroups: ").
                append(groups.size()).append('\n');
        out.append("total number of bytes reservable: ").
                append(groups.stream().mapToLong(LinkGroup::getAvailableSpace).sum()).append('\n');
        out.append("total number of bytes reserved  : ").
                append(groups.stream().mapToLong(LinkGroup::getReservedSpace).sum()).append('\n');

        return out.toString();
    }

    private void updateTransfers()
    {
        Futures.addCallback(_collector.collectTransfers(),
                            new FutureCallback<List<Transfer>>()
                            {
                                @Override
                                public void onSuccess(List<Transfer> result)
                                {
                                    result.sort(new TransferCollector.ByDoorAndSequence());
                                    _transfers = result;
                                    _scheduler.schedule(PcellsCommand.this::updateTransfers, 2, TimeUnit.MINUTES);
                                }

                                @Override
                                public void onFailure(Throwable t)
                                {
                                    LOGGER.error("Possible bug detected. Please contact support@dcache.org.", t);
                                    _scheduler.schedule(PcellsCommand.this::updateTransfers, 30, TimeUnit.SECONDS);
                                }
                            }, _executor);
    }

    private String listTransfers(List<Transfer> transfers)
    {
        long now = System.currentTimeMillis();
        StringBuilder sb  = new StringBuilder();

        for (Transfer io : transfers) {
            List<String> args = new ArrayList<>();
            args.add(io.door().getCellName());
            args.add(io.door().getDomainName());
            args.add(String.valueOf(io.session().getSerialId()));
            args.add(io.door().getProtocolFamily() + "-" + io.door().getProtocolVersion());
            args.add(io.door().getOwner());
            args.add(io.door().getProcess());
            args.add(Objects.toString(io.session().getPnfsId(), ""));
            args.add(Objects.toString(io.session().getPool(), ""));
            args.add(io.session().getReplyHost());
            args.add(Objects.toString(io.session().getStatus(), ""));
            args.add(String.valueOf(now - io.session().getWaitingSince()));

            IoJobInfo mover = io.mover();
            if (mover == null) {
                args.add("No-mover()-Found");
            } else {
                args.add(mover.getStatus());
                if (mover.getStartTime() > 0L) {
                    long transferTime     = mover.getTransferTime();
                    long bytesTransferred = mover.getBytesTransferred();
                    args.add(String.valueOf(transferTime));
                    args.add(String.valueOf(bytesTransferred));
                    args.add(String.valueOf(transferTime > 0 ? ((double) bytesTransferred / (double) transferTime) : 0));
                    args.add(String.valueOf(now - mover.getStartTime()));
                }
            }
            sb.append(new Args(args)).append('\n');
        }
        return sb.toString();
    }
}
