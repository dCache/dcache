package org.dcache.pinmanager;

import java.io.IOException;
import java.io.File;
import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Set;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.Future;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.dcache.cells.AbstractCellComponent;
import org.dcache.cells.CellCommandListener;
import org.dcache.cells.CellStub;
import org.dcache.pinmanager.model.Pin;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.PnfsHandler;
import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.DCapProtocolInfo;
import org.dcache.services.pinmanager1.PinManagerMovePinMessage;
import org.dcache.pinmanager.PinManagerPinMessage;
import org.dcache.pinmanager.PinManagerUnpinMessage;
import org.dcache.pinmanager.PinManagerExtendPinMessage;
import org.dcache.vehicles.FileAttributes;
import org.dcache.namespace.FileAttribute;

import java.util.concurrent.ExecutionException;

import org.springframework.beans.factory.annotation.Required;

import dmg.util.Args;

public class PinManagerCLI
    extends AbstractCellComponent
    implements CellCommandListener
{
    private final static AtomicInteger _counter =
        new AtomicInteger(0);

    private final Map<Integer,BulkJob> _jobs =
        new ConcurrentHashMap<Integer,BulkJob>();

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
            new DCapProtocolInfo("DCap", 3, 0, "localhost", 0);
        PinManagerPinMessage message =
            new PinManagerPinMessage(attributes, protocolInfo, requestId, lifetime);
        return _pinProcessor.messageArrived(message);
    }

    public final static String hh_pin =
        "<pnfs-id> <seconds> # pin a file by pnfsid for <seconds> seconds";
    public final static String fh_pin =
        "Pins a file to disk for some time. A file may be pinned forever by\n" +
        "specifying a lifetime of -1. Pinning a file may involve staging it\n" +
        "or copying it from one pool to another. For that reason pinning may\n" +
        "take awhile and the pin command may time out. The pin request will\n" +
        "however stay active and progress may be tracked by listing the pins\n" +
        "on the file.";
    public String ac_pin_$_2(Args args)
        throws NumberFormatException, CacheException,
               ExecutionException, InterruptedException
    {
        PnfsId pnfsId = new PnfsId(args.argv(0));
        long lifetime = Long.parseLong(args.argv(1));
        if (lifetime != -1) {
            lifetime *= 1000;
        }
        PinManagerPinMessage message = pin(pnfsId, null, lifetime).get();
        if (message.getExpirationTime() == null) {
            return String.format("[%d] %s pinned", message.getPinId(), pnfsId);
        } else {
            return String.format("[%d] %s pinned until %tc",
                                 message.getPinId(), pnfsId,
                                 message.getExpirationTime());
        }
    }

    public final static String hh_unpin =
        "<pin-ref> <pnfs-id> # unpin a file; <pin-ref> is either pin id or '*'";
    public final static String fh_unpin =
        "Unpin a previously pinned file. Either a specific pin or all pins on\n" +
        "a specific file can be removed.";
    public String ac_unpin_$_2(Args args)
        throws NumberFormatException, CacheException,
               ExecutionException, InterruptedException
    {
        PnfsId pnfsId = new PnfsId(args.argv(1));
        PinManagerUnpinMessage message =
            new PinManagerUnpinMessage(pnfsId);
        if (!args.argv(0).equals("*")) {
            message.setPinId(Long.parseLong(args.argv(0)));
        }

        _unpinProcessor.messageArrived(message);

        return "The pin is now scheduled for removal";
    }

    public final static String hh_extend =
        "<pin-id> <pnfs-id> <seconds> # unpin a file";
    public final static String fh_extend =
        "Extend the lifetime of an existing pin. A pin with a lifetime of -1\n" +
        "will never expire and has to be unpinned explicitly. The lifetime\n" +
        "of a pin can only be extended, not shortened.";
    public String ac_extend_$_3(Args args)
        throws NumberFormatException, CacheException,
               ExecutionException, InterruptedException
    {
        long pinId = Long.parseLong(args.argv(0));
        PnfsId pnfsId = new PnfsId(args.argv(1));
        long lifetime = Long.parseLong(args.argv(2));
        if (lifetime != -1) {
            lifetime *= 1000;
        }
        Set<FileAttribute> attributes =
            PinManagerExtendPinMessage.getRequiredAttributes();
        FileAttributes fileAttributes =
            _pnfs.getFileAttributes(pnfsId, attributes);
        PinManagerExtendPinMessage message =
            new PinManagerExtendPinMessage(fileAttributes, pinId, lifetime);
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

    public final static String hh_ls =
        "[<pin-id>|<pnfs-id>] # lists all pins or a specified pin by pin id or pnfsid" ;
    public final static String fh_ls =
        "Lists all pins or a specified pin by pin id or pnfsid.";
    public String ac_ls_$_0_1(Args args)
    {
        Collection<Pin> pins;
        if (args.argc() > 0) {
            String id = args.argv(0);
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

    public final static String hh_bulk_pin =
        "<file> <seconds> # pin pnfsids from <file> for <seconds>";
    public final static String fh_bulk_pin =
        "Pin a list of PNFS IDs from a file for a specified number of\n"+
        "seconds. Each line of the file must be a PNFS ID.\n";
    public Object ac_bulk_pin_$_2(Args args)
        throws IOException, InterruptedException, CacheException
    {
        File file = new File(args.argv(0));
        long lifetime = Long.parseLong(args.argv(1));
        if (lifetime != -1) {
            lifetime *= 1000;
        }

        BulkJob job = new BulkJob(parse(file), lifetime);
        _jobs.put(job.getId(), job);
        new Thread(job, "BulkPin-" + job.getId()).start();
        return job.getJobDescription();
    }

    public final static String hh_bulk_unpin =
        "<file> # unpin pnfsids from <file>";
    public final static String fh_bulk_unpin =
        "Unpin a list of PNFS IDs from a file. Each line of the file\n" +
        "must be a PNFS ID.";
    public Object ac_bulk_unpin_$_1(final Args args)
        throws IOException, InterruptedException, CacheException
    {
        File file = new File(args.argv(0));
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


    public final static String hh_bulk_clear = "# Removes completed jobs";
    public final static String fh_bulk_clear =
        "Removes completed jobs. For reference, information\n" +
        "about background jobs is kept until explicitly cleared.\n";
    public String ac_bulk_clear(Args args)
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

    public final static String hh_bulk_cancel =
        "<id> # cancels a background job";
    public final static String fh_bulk_cancel =
        "Cancels a background job. Notice that a cancelling a bulk job will\n" +
        "cause all pins already created by the job to be released.";
    public String ac_bulk_cancel_$_1(Args args)
        throws NumberFormatException, CacheException
    {
        BulkJob job = _jobs.get(Integer.parseInt(args.argv(0)));
        if (job == null) {
            return "No such job";
        }
        job.cancel();
        return job.getJobDescription();
    }

    public final static String hh_bulk_ls =
        "[<id>] # list background jobs";
    public final static String fh_bulk_ls =
        "Lists background jobs. If a job id is specified then additional" +
        "status information about the job is provided.";
    public String ac_bulk_ls_$_0_1(Args args)
        throws NumberFormatException, InterruptedException
    {
        if (args.argc() == 0) {
            StringBuilder sb = new StringBuilder();
            for (Map.Entry<Integer,BulkJob> entry: _jobs.entrySet()) {
                int id = entry.getKey();
                BulkJob job = entry.getValue();
                sb.append(job.getJobDescription()).append('\n');
            }
            return sb.toString();
        } else {
            BulkJob job = _jobs.get(Integer.parseInt(args.argv(0)));
            if (job == null) {
                return "No such job";
            }
            return job.toString();
        }
    }

    private List<PnfsId> parse(File file)
        throws IOException
    {
        List<PnfsId> list = new ArrayList<PnfsId>();
        BufferedReader reader = new BufferedReader(new FileReader(file));
        try {
            String line;
            while ((line = reader.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty() || line.startsWith("#")) continue;
                list.add(new PnfsId(line));
            }
        } catch (IllegalArgumentException e) {
            throw new IOException("Invalid file format: " + e.getMessage());
        } finally {
            reader.close();
        }
        return list;
    }

    private class BulkJob
        implements Runnable
    {
        protected final Map<PnfsId,Future<PinManagerPinMessage>> _tasks =
            new HashMap<PnfsId,Future<PinManagerPinMessage>>();
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