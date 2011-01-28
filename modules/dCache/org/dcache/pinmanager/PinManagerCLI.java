package org.dcache.pinmanager;

import java.io.IOException;
import java.io.File;
import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Set;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.List;
import java.util.Collection;
import java.util.concurrent.Future;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
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
    private final Map<Integer,Future<String>> _jobs =
        new ConcurrentHashMap<Integer,Future<String>>();

    private PnfsHandler _pnfs;
    private PinManager _pinManager;
    private PinDao _dao;
    private PinRequestProcessor _pinProcessor;
    private UnpinRequestProcessor _unpinProcessor;
    private MovePinRequestProcessor _moveProcessor;
    private ExecutorService _executor;

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
    public void setExecutor(ExecutorService executor)
    {
        _executor = executor;
    }

    @Required
    public void setDao(PinDao dao)
    {
        _dao = dao;
    }

    private PinManagerPinMessage pin(PnfsId pnfsId, long lifetime)
        throws CacheException, ExecutionException, InterruptedException
    {
        Set<FileAttribute> attributes =
            PinManagerPinMessage.getRequiredAttributes();
        FileAttributes fileAttributes =
            _pnfs.getFileAttributes(pnfsId, attributes);
        DCapProtocolInfo protocolInfo =
            new DCapProtocolInfo("DCap", 3, 0, "localhost", 0);
        PinManagerPinMessage message =
            new PinManagerPinMessage(fileAttributes, protocolInfo, null, lifetime);
        return _pinProcessor.messageArrived(message).get();
    }

    private void unpin(PnfsId pnfsId)
        throws CacheException
    {
        Set<FileAttribute> attributes =
            PinManagerUnpinMessage.getRequiredAttributes();
        FileAttributes fileAttributes =
            _pnfs.getFileAttributes(pnfsId, attributes);
        PinManagerUnpinMessage message =
            new PinManagerUnpinMessage(fileAttributes);
        _unpinProcessor.messageArrived(message);
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
        PinManagerPinMessage message = pin(pnfsId, lifetime);
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
        Set<FileAttribute> attributes =
            PinManagerUnpinMessage.getRequiredAttributes();
        FileAttributes fileAttributes =
            _pnfs.getFileAttributes(pnfsId, attributes);
        PinManagerUnpinMessage message =
            new PinManagerUnpinMessage(fileAttributes);
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
        "Pin a list of pnfsids from a file for a specified number of\n"+
        "seconds read a list of pnfsids to pin from a file each line\n"+
        "in a file is a pnfsid.\n";
    public Object ac_bulk_pin_$_2(Args args)
        throws IOException, InterruptedException, CacheException
    {
        File file = new File(args.argv(0));
        long lifetime = Long.parseLong(args.argv(1));
        if (lifetime != -1) {
            lifetime *= 1000;
        }
        return addJob(new BulkPinJob(parse(file), lifetime));
    }

    public final static String hh_bulk_unpin =
        "<file> # unpin pnfsids from <file>";
    public final static String fh_bulk_unpin =
        "Unpin a list of pnfsids from a file read a list of pnfsids\n"+
        "to pin from a file each line in a file is a pnfsid.\n";
    public Object ac_bulk_unpin_$_1(final Args args)
        throws IOException, InterruptedException, CacheException
    {
        File file = new File(args.argv(0));
        return addJob(new BulkUnpinJob(parse(file)));
    }


    public final static String hh_bulk_clear = "# Removes completed jobs";
    public final static String fh_bulk_clear =
        "Removes completed jobs. For reference, information\n" +
        "about background jobs is kept until explicitly cleared.\n";
    public String ac_jobs_clear(Args args)
    {
        int count = 0;
        Iterator<Future<String>> i = _jobs.values().iterator();
        while (i.hasNext()) {
            if (i.next().isDone()) {
                i.remove();
                count++;
            }
        }
        return String.format("%d jobs removed", count);
    }

    public final static String hh_jobs_cancel =
        "<id> # cancels a background job";
    public final static String fh_jobs_cancel =
        "Cancels a background job. Notice that a job is not transactional.\n" +
        "For this reason a cancelled job may already have pinned or unpinned\n" +
        "some files.\n";
    public String ac_jobs_cancel_$_1(Args args)
        throws NumberFormatException
    {
        int id = Integer.parseInt(args.argv(0));
        Future<String> future = _jobs.get(id);
        if (future == null) {
            return "No such job";
        }
        future.cancel(true);
        return getJobDescription(id, future);
    }

    public final static String hh_jobs_ls =
        "# list background jobs";
    public final static String fh_jobs_ls =
        "Lists all background jobs.";
    public String ac_jobs_ls(Args args)
        throws NumberFormatException, InterruptedException
    {
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<Integer,Future<String>> entry: _jobs.entrySet()) {
            Future<String> future = entry.getValue();
            sb.append(getJobDescription(entry.getKey(), future)).append('\n');
            if (future.isDone()) {
                try {
                    sb.append(future.get()); // Bulk job output is \n terminated
                } catch (ExecutionException e) {
                    sb.append(e.getCause().toString()).append('\n');
                }
            }
        }
        return sb.toString();
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

    private String addJob(Callable<String> job)
    {
        int id = _counter.incrementAndGet();
        Future<String> future = _executor.submit(job);
        _jobs.put(id, future);
        return getJobDescription(id, future);
    }

    private String getJobDescription(long id, Future<?> future)
    {
        return String.format("[%d] %s", id, getState(future));
    }

    private String getState(Future<?> future)
    {
        if (future.isCancelled()) {
            return "CANCELLED";
        } else if (future.isDone()) {
            return "DONE";
        } else {
            return "PROCESSING";
        }
    }

    private class BulkPinJob implements Callable<String>
    {
        protected List<PnfsId> _ids;
        private long _lifetime;

        public BulkPinJob(List<PnfsId> ids, long lifetime)
            throws CacheException
        {
            _ids = ids;
            _lifetime = lifetime;
        }

        @Override
        public String call()
            throws InterruptedException
        {
            StringBuilder out = new StringBuilder();
            for (PnfsId id: _ids) {
                try {
                    pin(id, _lifetime);
                } catch (ExecutionException e) {
                    out.append(id).append(": ").append(e.getCause().getMessage()).append('\n');
                } catch (CacheException e) {
                    out.append(id).append(": ").append(e.getMessage()).append('\n');
                }
            }
            return out.toString();
        }
    }

    private class BulkUnpinJob implements Callable<String>
    {
        protected List<PnfsId> _ids;

        public BulkUnpinJob(List<PnfsId> ids)
            throws CacheException
        {
            _ids = ids;
        }

        @Override
        public String call()
        {
            StringBuilder out = new StringBuilder();
            for (PnfsId id: _ids) {
                try {
                    unpin(id);
                } catch (CacheException e) {
                    out.append(id).append(": ").append(e.getMessage()).append('\n');
                }
            }
            return out.toString();
        }
    }
}