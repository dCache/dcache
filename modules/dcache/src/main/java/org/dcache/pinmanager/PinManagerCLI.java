package org.dcache.pinmanager;

import org.springframework.beans.factory.annotation.Required;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.PnfsHandler;
import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.DCapProtocolInfo;

import dmg.util.command.Argument;
import dmg.util.command.Command;

import dmg.cells.nucleus.AbstractCellComponent;
import dmg.cells.nucleus.CellCommandListener;
import org.dcache.cells.CellStub;
import org.dcache.namespace.FileAttribute;
import org.dcache.pinmanager.model.Pin;
import org.dcache.vehicles.FileAttributes;

public class PinManagerCLI
    extends AbstractCellComponent
    implements CellCommandListener
{
    private final static AtomicInteger _counter =
        new AtomicInteger(0);

    private final Map<Integer,BulkJob> _jobs =
        new ConcurrentHashMap<>();

    private PnfsHandler _pnfs;
    private PinManager _pinManager;
    private PinDao _dao;
    private PinRequestProcessor _pinProcessor;
    private UnpinRequestProcessor _unpinProcessor;
    private MovePinRequestProcessor _moveProcessor;

    @Required
    public void setPnfsStub(CellStub stub)
    {
        _pnfs = new PnfsHandler(stub);
    }

    @Required
    public void setPinManager(PinManager pinManager)
    {
        _pinManager = pinManager;
    }

    @Required
    public void setPinProcessor(PinRequestProcessor processor)
    {
        _pinProcessor = processor;
    }

    @Required
    public void setUnpinProcessor(UnpinRequestProcessor processor)
    {
        _unpinProcessor = processor;
    }

    @Required
    public void setMoveProcessor(MovePinRequestProcessor processor)
    {
        _moveProcessor = processor;
    }

    @Required
    public void setDao(PinDao dao)
    {
        _dao = dao;
    }

    private Future<PinManagerPinMessage>
        pin(PnfsId pnfsId, String requestId, long lifetime)
        throws CacheException
    {
        FileAttributes attributes = new FileAttributes();
        attributes.setPnfsId(pnfsId);
        DCapProtocolInfo protocolInfo =
            new DCapProtocolInfo("DCap", 3, 0, new InetSocketAddress("localhost", 0));
        PinManagerPinMessage message =
            new PinManagerPinMessage(attributes, protocolInfo, requestId, lifetime);
        return _pinProcessor.messageArrived(message);
    }

    @Command(name="pin", hint="pin a file to disk",
    description = "Pins a file to disk for some time. A file may be pinned forever by " +
            "specifying a lifetime of -1. Pinning a file may involve staging it " +
            "or copying it from one pool to another. For that reason pinning may " +
            "take awhile and the pin command may time out. The pin request will " +
            "however stay active and progress may be tracked by listing the pins " +
            "on the file.")
    public class PinCommand implements Callable<String>
    {
        @Argument(index=0)
        PnfsId pnfsId;

        @Argument(index=1, metaVar = "seconds")
        long lifetime;

        @Override
        public String call()
                throws CacheException, ExecutionException, InterruptedException
        {
            long millis = (lifetime == -1) ? -1 : TimeUnit.SECONDS.toMillis(lifetime);
            PinManagerPinMessage message = pin(pnfsId, null, millis).get();
            if (message.getExpirationTime() == null) {
                return String.format("[%d] %s pinned", message.getPinId(), pnfsId);
            } else {
                return String.format("[%d] %s pinned until %tc",
                        message.getPinId(), pnfsId,
                        message.getExpirationTime());
            }
        }
    }

    @Command(name="unpin", hint="unpin a file",
            description = "Unpin a previously pinned file. Either a specific pin or all " +
                    "pins on a specific file can be removed.")
    public class UnpinCommand implements Callable<String>
    {
        @Argument(index = 0, valueSpec="*|PIN")
        String pin;

        @Argument(index = 1)
        PnfsId pnfsId;

        @Override
        public String call() throws NumberFormatException, CacheException
        {
            PinManagerUnpinMessage message = new PinManagerUnpinMessage(pnfsId);
            if (!pin.equals("*")) {
                message.setPinId(Long.parseLong(pin));
            }
            _unpinProcessor.messageArrived(message);
            return "The pin is now scheduled for removal";
        }
    }

    @Command(name="extend", hint="extend lifetime of a pin",
            description = "Extend the lifetime of an existing pin. A pin with a lifetime of -1 " +
                    "will never expire and has to be unpinned explicitly. The lifetime " +
                    "of a pin can only be extended, not shortened.")
    public class ExtendCommand implements Callable<String>
    {
        @Argument(index = 0)
        long pin;

        @Argument(index = 1)
        PnfsId pnfsId;

        @Argument(index = 2, metaVar = "seconds")
        long lifetime;

        @Override
        public String call() throws CacheException, InterruptedException
        {
            long millis = (lifetime == -1) ? -1 : TimeUnit.SECONDS.toMillis(lifetime);

            Set<FileAttribute> attributes =
                    PinManagerExtendPinMessage.getRequiredAttributes();
            FileAttributes fileAttributes =
                    _pnfs.getFileAttributes(pnfsId, attributes);
            PinManagerExtendPinMessage message =
                    new PinManagerExtendPinMessage(fileAttributes, pin, millis);
            message = _moveProcessor.messageArrived(message);
            if (message.getExpirationTime() == null) {
                return String.format("[%d] %s pinned",
                        message.getPinId(), pnfsId);
            } else {
                return String.format("[%d] %s pinned until %tc",
                        message.getPinId(), pnfsId,
                        message.getExpirationTime());
            }
        }
    }

    @Command(name="ls", hint="list pins",
            description = "Lists all pins or a specified pin by pin id or PNFSID.")
    public class ListCommand implements Callable<String>
    {
        @Argument(index = 0, required = false, valueSpec="PIN|PNFSID")
        String id;

        @Override
        public String call() throws IllegalArgumentException
        {
            Collection<Pin> pins;
            if (id != null) {
                if (!PnfsId.isValid(id)) {
                    Pin pin = _dao.getPin(Long.parseLong(id));
                    return (pin == null) ? "" : pin.toString();
                }
                pins = _dao.getPins(new PnfsId(id));
            } else {
                pins = _dao.getPins();
            }

            StringBuilder out = new StringBuilder();
            for (Pin pin: pins) {
                out.append(pin).append("\n");
            }
            out.append("total ").append(pins.size());
            return out.toString();

        }
    }

    @Command(name="bulk pin", hint="pin several files",
            description = "Pin a list of PNFS IDs from FILE for a specified number of " +
                    "seconds. Each line FILE must be a PNFS ID.")
    public class BulkPinCommand implements Callable<String>
    {
        @Argument(index = 0)
        File file;

        @Argument(index = 1, metaVar="seconds")
        long lifetime;

        @Override
        public String call() throws IOException
        {
            long millis = (lifetime == -1) ? -1 : TimeUnit.SECONDS.toMillis(lifetime);

            BulkJob job = new BulkJob(parse(file), millis);
            _jobs.put(job.getId(), job);
            new Thread(job, "BulkPin-" + job.getId()).start();
            return job.getJobDescription();
        }
    }

    @Command(name = "bulk unpin", hint = "unpin several files",
            description = "Unpin a list of PNFS IDs from FILE. Each line of FILE " +
                    "must be a PNFS ID.")
    public class BulkUnpinCommand implements Callable<String>
    {
        @Argument(index = 0)
        File file;

        @Override
        public String call() throws IOException
        {
            StringBuilder out = new StringBuilder();
            for (PnfsId pnfsId: parse(file)) {
                try {
                    PinManagerUnpinMessage message =
                            new PinManagerUnpinMessage(pnfsId);
                    _unpinProcessor.messageArrived(message);
                } catch (CacheException e) {
                    out.append(pnfsId).append(": ").append(e.getMessage()).append('\n');
                }
            }
            return out.toString();
        }
    }

    @Command(name = "bulk clear", hint = "remove completed bulk jobs",
            description = "Removes completed jobs. For reference, information " +
                    "about background jobs is kept until explicitly cleared.")
    public class BulkClearCommand implements Callable<String>
    {
        @Override
        public String call() throws Exception
        {
            int count = 0;
            Iterator<BulkJob> i = _jobs.values().iterator();
            while (i.hasNext()) {
                if (i.next().isDone()) {
                    i.remove();
                    count++;
                }
            }
            return String.format("%d jobs removed", count);
        }
    }

    @Command(name = "bulk cancel", hint="cancel bulk job",
            description = "Cancels a background job. Note that cancelling a bulk job will " +
                    "cause all pins already created by the job to be released.")
    public class BulkCancelCommand implements Callable<String>
    {
        @Argument
        int id;

        @Override
        public String call() throws CacheException
        {
            BulkJob job = _jobs.get(id);
            if (job == null) {
                return "No such job";
            }
            job.cancel();
            return job.getJobDescription();
        }
    }

    @Command(name = "bulk ls", hint = "list bulk jobs",
            description = "Lists background jobs. If a job id is specified then additional " +
                    "status information about the job is provided.")
    public class BulkListCommand implements Callable<String>
    {
        @Argument(required = false)
        Integer id;

        @Override
        public String call()
        {
            if (id == 0) {
                StringBuilder sb = new StringBuilder();
                for (Map.Entry<Integer,BulkJob> entry: _jobs.entrySet()) {
                    int id = entry.getKey();
                    BulkJob job = entry.getValue();
                    sb.append(job.getJobDescription()).append('\n');
                }
                return sb.toString();
            } else {
                BulkJob job = _jobs.get(id);
                if (job == null) {
                    return "No such job";
                }
                return job.toString();
            }

        }
    }

    private List<PnfsId> parse(File file)
        throws IOException
    {
        List<PnfsId> list = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
            String line;
            while ((line = reader.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty() || line.startsWith("#")) {
                    continue;
                }
                list.add(new PnfsId(line));
            }
        } catch (IllegalArgumentException e) {
            throw new IOException("Invalid file format: " + e.getMessage());
        }

        return list;
    }

    private class BulkJob
        implements Runnable
    {
        protected final Map<PnfsId,Future<PinManagerPinMessage>> _tasks =
            new HashMap<>();
        private final StringBuilder _errors =
            new StringBuilder();
        protected final String _requestId = UUID.randomUUID().toString();
        protected final int _id;
        protected final long _lifetime;
        protected boolean _cancelled;

        public BulkJob(List<PnfsId> files, long lifetime)
        {
            _id = _counter.incrementAndGet();
            _lifetime = lifetime;
            for (PnfsId pnfsId: files) {
                _tasks.put(pnfsId, null);
            }
        }

        @Override
        public void run()
        {
            List<PnfsId> list = new ArrayList(_tasks.keySet());
            for (PnfsId pnfsId: list) {
                try {
                    _tasks.put(pnfsId, pin(pnfsId, _requestId, _lifetime));
                } catch (CacheException e) {
                    _tasks.remove(pnfsId);
                    _errors.append("    ").append(pnfsId).
                        append(": ").append(e.getMessage()).append('\n');
                } catch (RuntimeException e) {
                    _tasks.remove(pnfsId);
                    _errors.append("    ").append(pnfsId).
                        append(": ").append(e.toString()).append('\n');
                }
            }
        }

        public int getId()
        {
            return _id;
        }

        public synchronized void cancel()
            throws CacheException
        {
            if (!_cancelled) {
                for (PnfsId pnfsId: _tasks.keySet()) {
                    PinManagerUnpinMessage message =
                        new PinManagerUnpinMessage(pnfsId);
                    message.setRequestId(_requestId);
                    _unpinProcessor.messageArrived(message);
                }
                _cancelled = true;
            }
        }

        public synchronized boolean isCancelled()
        {
            return _cancelled;
        }

        public boolean isDone()
        {
            for (Future<PinManagerPinMessage> task: _tasks.values()) {
                if (task == null || !task.isDone()) {
                    return false;
                }
            }
            return true;
        }

        public String getJobDescription()
        {
            String state;
            if (isCancelled()) {
                state = "CANCELLED";
            } else if (isDone()) {
                state = "DONE";
            } else {
                state = "PROCESSING";
            }
            return String.format("[%d] %s", _id, state);
        }

        @Override
        public String toString()
        {
            try {
                StringBuilder out = new StringBuilder();
                for (Map.Entry<PnfsId,Future<PinManagerPinMessage>> entry: _tasks.entrySet()) {
                    out.append("  ").append(entry.getKey()).append(": ");
                    try {
                        Future<PinManagerPinMessage> future = entry.getValue();
                        if (future == null) {
                            out.append("INITIALIZING");
                        } else if (!future.isDone()) {
                            out.append("PROCESSING");
                        } else if (isCancelled()) {
                            out.append("CANCELLED");
                        } else {
                            future.get();
                            out.append("DONE");
                        }
                    } catch (ExecutionException e) {
                        out.append(e.getMessage());
                    }
                    out.append('\n');
                }

                if (_errors.length() > 0) {
                    out.append("Failed during initialization:\n");
                    out.append(_errors);
                }

                return out.toString();
            } catch (InterruptedException e) {
                return e.toString();
            }
        }
    }
}
