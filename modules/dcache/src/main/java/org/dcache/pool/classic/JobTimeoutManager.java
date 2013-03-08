package org.dcache.pool.classic;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import diskCacheV111.vehicles.IoJobInfo;
import diskCacheV111.vehicles.JobInfo;

import dmg.util.Args;

import org.dcache.cells.AbstractCellComponent;
import org.dcache.cells.CellCommandListener;

class SchedulerEntry
{
    private final String _name;
    private IoScheduler _scheduler;
    private long _lastAccessed;
    private long _total;

    SchedulerEntry(String name)
    {
        _name = name;
    }

    String getName()
    {
        return _name;
    }

    synchronized void setScheduler(IoScheduler scheduler)
    {
        _scheduler = scheduler;
    }

    synchronized IoScheduler getScheduler()
    {
        return _scheduler;
    }

    synchronized void setLastAccessed(long lastAccessed)
    {
        _lastAccessed = lastAccessed;
    }

    synchronized long getLastAccessed()
    {
        return _lastAccessed;
    }

    synchronized void setTotal(long total)
    {
        _total = total;
    }

    synchronized long getTotal()
    {
        return _total;
    }

    synchronized boolean isLastAccessExpired(JobInfo job, long now)
    {
        long lastAccessed =
            job instanceof IoJobInfo ?
            ((IoJobInfo)job).getLastTransferred() :
            0;

        return getLastAccessed() > 0L && lastAccessed > 0L && (now - lastAccessed) > getLastAccessed();
    }

    synchronized boolean isTotalTimeExpired(JobInfo job, long now)
    {
        long started = job.getStartTime();
        return getTotal() > 0L && started > 0L && (now - started) > getTotal();
    }
}

public class JobTimeoutManager
    extends AbstractCellComponent
    implements Runnable,
               CellCommandListener
{
    private final static Logger _log =
        LoggerFactory.getLogger(JobTimeoutManager.class);

    private final List<SchedulerEntry> _schedulers
        = new CopyOnWriteArrayList<>();
    private final Thread _worker;

    public JobTimeoutManager()
    {
        _worker = new Thread(this , "JobTimeoutManager");
    }

    public synchronized void start()
    {
        if (_worker.isAlive()) {
            throw new IllegalStateException("Already running");
        }
        _worker.start();
    }

    public void addScheduler(String type, IoScheduler scheduler)
    {
        _log.trace("Adding scheduler : " + type);
        SchedulerEntry entry = findOrCreate(type);
        entry.setScheduler(scheduler);
    }

    @Override
    public void printSetup(PrintWriter pw)
    {
        for (SchedulerEntry entry: _schedulers) {
            pw.println("jtm set timeout -queue=" + entry.getName() +
                       " -lastAccess=" + (entry.getLastAccessed() / 1000L) +
                       " -total=" + (entry.getTotal() / 1000L));
        }
    }

    @Override
    public void getInfo(PrintWriter pw)
    {
        pw.println("Job Timeout Manager");

        for (SchedulerEntry entry: _schedulers) {
            pw.println("  " + entry.getName() +
                       " (lastAccess=" + (entry.getLastAccessed() / 1000L) +
                       ";total=" + (entry.getTotal() / 1000L) + ")");
        }
    }

    public static final String hh_jtm_go = "trigger the worker thread";
    public synchronized String ac_jtm_go(Args args)
    {
        notifyAll();
        return "";
    }

    public static final String hh_jtm_ls = "list queues";
    public String ac_jtm_ls(Args args)
    {
        StringBuilder sb = new StringBuilder();
        for (SchedulerEntry entry : _schedulers) {
            sb.append(entry.getName()).append(" ");
        }
        return sb.toString();
    }

    private SchedulerEntry find(String name)
    {
        for (SchedulerEntry entry : _schedulers) {
            if (entry.getName().equals(name)) {
                return entry;
            }
        }
        return null;
    }

    private synchronized SchedulerEntry findOrCreate(String name)
    {
        if (name == null) {
            throw new IllegalArgumentException("null argument not allowed");
        }

        SchedulerEntry entry = find(name);
        if (entry == null) {
            entry = new SchedulerEntry(name);
            _schedulers.add(entry);
        }
        return entry;
    }

    public static final String hh_jtm_set_timeout =
        "[-total=<timeout/sec>] [-lastAccess=<timeout/sec>] [-queue=<queueName>]" ;
    public String ac_jtm_set_timeout(Args args)
    {
        String  queue         = args.getOpt("queue");
        String  lastAccessStr = args.getOpt("lastAccess");
        String  totalStr      = args.getOpt("total");

        long lastAccess = lastAccessStr == null ? -1 : (Long.parseLong(lastAccessStr)*1000L) ;
        long total      = totalStr      == null ? -1 : (Long.parseLong(totalStr)*1000L) ;

        if (queue == null) {
            for (SchedulerEntry entry: _schedulers) {
                if (lastAccess >= 0L) {
                    entry.setLastAccessed(lastAccess);
                }
                if (total >= 0L) {
                    entry.setTotal(total);
                }
            }
        } else {
            SchedulerEntry entry = findOrCreate(queue);
            if (lastAccess >= 0L) {
                entry.setLastAccessed(lastAccess);
            }
            if (total >= 0L) {
                entry.setTotal(total);
            }
        }
        return "";
    }

    @Override
    public void run()
    {
        try {
            while (!Thread.currentThread().isInterrupted()) {
                synchronized (this) {
                    wait(120000L);
                }

                long now = System.currentTimeMillis();
                for (SchedulerEntry entry: _schedulers) {
                    IoScheduler jobs = entry.getScheduler();
                    if (jobs == null) {
                        continue;
                    }

                    for (JobInfo info: jobs.getJobInfos()) {
                        int jobId = (int)info.getJobId();
                        if (entry.isLastAccessExpired(info, now)) {
                            _log.warn("Killing mover {}/{} because it has been idle for {} seconds",
                                    entry.getName(), jobId, (now - ((IoJobInfo) info).getLastTransferred()) / 1000);
                            jobs.cancel(jobId);
                        } else if (entry.isTotalTimeExpired(info, now)) {
                            _log.warn("Killing mover {}/{} because it is {} seconds old",
                                    entry.getName(), jobId, (now - info.getStartTime()) / 1000);
                            jobs.cancel(jobId);
                        }
                    }
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
