package org.dcache.services.pinmanager1;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.BufferedReader;
import java.io.FileReader;
import java.util.List;
import java.util.Map;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.dcache.cells.CellCommandListener;

import dmg.util.Args;

import diskCacheV111.util.PnfsId;
import diskCacheV111.util.CacheException;

import org.springframework.beans.factory.annotation.Required;

import static org.dcache.services.pinmanager1.PinManagerJobState.*;

public class PinManagerCLI
    implements CellCommandListener
{
    private static final Logger logger =
        LoggerFactory.getLogger(PinManagerCLI.class);

    private final static AtomicInteger _counter =
        new AtomicInteger(0);
    private final Map<Integer,InteractiveJob> _jobs =
        new ConcurrentHashMap<Integer,InteractiveJob>();

    private PinManager _pinManager;

    @Required
    public void setPinManager(PinManager pinManager)
    {
        _pinManager = pinManager;
    }

    private String addJob(InteractiveJob job)
    {
        int id = _counter.incrementAndGet();
        _jobs.put(id, job);
        return String.format("[%d] %s", id, job);
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
        "[-force] <file> # unpin pnfsids from <file>";
    public final static String fh_bulk_unpin =
        "Unpin a list of pnfsids from a file read a list of pnfsids\n"+
        "to pin from a file each line in a file is a pnfsid.\n";
    public Object ac_bulk_unpin_$_1(final Args args)
        throws IOException, InterruptedException, CacheException
    {
        boolean force = args.getOpt("force") != null;
        File file = new File(args.argv(0));
        return addJob(new BulkUnpinJob(parse(file), force));
    }

    public final static String hh_pin_pnfsid =
        "<pnfsId> <seconds> # pin a file by pnfsid for <seconds> seconds";
    public String ac_pin_pnfsid_$_2(Args args)
        throws NumberFormatException, CacheException
    {
        PnfsId pnfsId = new PnfsId(args.argv(0));
        long lifetime = Long.parseLong(args.argv(1));
        if (lifetime != -1) {
            lifetime *= 1000;
        }
        return addJob(_pinManager.pin(pnfsId, lifetime));
    }

    public final static String hh_unpin =
        "[-force] [<pinRequestId>] <pnfsId> " +
        "# unpin a a file by pinRequestId and by pnfsId or just by pnfsId";
    public String ac_unpin_$_1_2(Args args)
        throws NumberFormatException, CacheException
    {
        boolean force = args.getOpt("force") != null;
        if (args.argc() == 1) {
            return addJob(_pinManager.unpin(new PnfsId(args.argv(0)), force));
        } else {
            return addJob(_pinManager.unpin(new PnfsId(args.argv(1)),
                                            Long.parseLong(args.argv(0)),
                                            force));
        }
    }

    public final static String hh_extend_lifetime =
        "<pinRequestId> <pnfsId> <seconds> " +
        "# extendlifetime of a pin  by pinRequestId and by pnfsId";
    public String ac_extend_lifetime_$_3(Args args)
        throws IllegalArgumentException, CacheException
    {
        long pinRequestId = Long.parseLong(args.argv(0));
        PnfsId pnfsId = new PnfsId(args.argv(1));
        long lifetime = Long.parseLong(args.argv(2)) * 1000;
        return addJob(_pinManager.extendLifetime(pnfsId, pinRequestId, lifetime));
    }

    public final static String hh_set_max_pin_duration =
        "<duration> # sets new max pin duration value in milliseconds, -1 for infinite";
    public String ac_set_max_pin_duration_$_1(Args args)
        throws NumberFormatException, CacheException
    {
        long duration = Long.parseLong(args.argv(0));
        if (duration < -1 || duration == 0) {
            return "Max pin duration value must be -1 or nonnegative";
        }

        _pinManager.setMaxPinDuration(duration);
        return "Pin duration set to " + duration + " milliseconds";
    }

    public final static String hh_get_max_pin_duration =
        "# gets current max pin duration value";
    public String ac_get_max_pin_duration_$_0(Args args)
    {
        return Long.toString(_pinManager.getMaxPinDuration()) + " milliseconds";
    }

    public final static String fh_jobs_clear =
        "Removes completed interactive jobs. For reference, information\n" +
        "about interactive jobs is kept until explicitly cleared.\n";
    public String ac_jobs_clear(Args args)
    {
        Iterator<InteractiveJob> i = _jobs.values().iterator();
        while (i.hasNext()) {
            InteractiveJob job = i.next();
            switch (job.getState()) {
            case COMPLETED:
                i.remove();
                break;
            default:
                break;
            }
        }
        return "";
    }

    public final static String hh_ls =
        "[id|pnfsId] # lists all pins or a specified pin by request id or pnfsid" ;
    public String ac_ls_$_0_1(Args args)
        throws NumberFormatException, PinDBException
    {
        if (args.argc() > 0) {
            String s = args.argv(0);
            if (PnfsId.isValid(s)) {
                return _pinManager.list(new PnfsId(s));
            } else {
                return _pinManager.list(Long.parseLong(s));
            }
        } else {
            return _pinManager.list();
        }
    }

    public final static String hh_jobs_ls =
        "[<id>]... # list all or specified jobs";
    public final static String fh_jobs_ls =
        "Lists all or specified or pin manager jobs.";
    public String ac_jobs_ls_$_0_99(Args args)
        throws NumberFormatException, PinDBException
    {
        StringBuilder sb = new StringBuilder();
        if (args.argc() == 0) {
            for (Map.Entry<Integer,InteractiveJob> e: _jobs.entrySet()) {
                sb.append('[').append(e.getKey()).append("] ");
                sb.append(e.getValue()).append('\n');
            }
        } else {
            for (int i = 0; i < args.argc(); i++) {
                int id = Integer.parseInt(args.argv(i));
                sb.append('[').append(id).append("] ");
                InteractiveJob job = _jobs.get(id);
                if (job != null) {
                    sb.append(job);
                } else {
                    sb.append("Not found");
                }
                sb.append('\n');
            }
        }
        return sb.toString();
    }

    private class BulkJob implements InteractiveJob
    {
        protected final List<InteractiveJob> jobs =
            new ArrayList<InteractiveJob>();

        public PinManagerJobState getState()
        {
            for (InteractiveJob job: jobs) {
                if (job.getState() == ACTIVE) {
                    return ACTIVE;
                }
            }
            return COMPLETED;
        }

        @Override
        public String toString()
        {
            StringBuilder sb = new StringBuilder();
            for (InteractiveJob job: jobs) {
                sb.append("  ").append(job).append('\n');
            }
            return sb.toString();
        }
    }

    public List<PnfsId> parse(File file)
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
        } finally {
            reader.close();
        }
        return list;
    }

    private class BulkPinJob extends BulkJob
    {
        public BulkPinJob(List<PnfsId> ids, long lifetime)
            throws CacheException
        {
            for (PnfsId id: ids) {
                jobs.add(_pinManager.pin(id, lifetime));
            }
        }

        @Override
        public String toString()
        {
            return "Pinning " + getState() + "\n" + super.toString();
        }
    }

    private class BulkUnpinJob extends BulkJob
    {
        public BulkUnpinJob(List<PnfsId> ids, boolean force)
            throws CacheException
        {
            for (PnfsId id: ids) {
                jobs.add(_pinManager.unpin(id, force));
            }
        }

        @Override
        public String toString() {
            return "Unpinning " + getState() + "\n" + super.toString();
        }
    }

}