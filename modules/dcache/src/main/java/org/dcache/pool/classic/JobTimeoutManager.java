package org.dcache.pool.classic;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import diskCacheV111.vehicles.IoJobInfo;
import diskCacheV111.vehicles.JobInfo;

import dmg.cells.nucleus.AbstractCellComponent;
import dmg.cells.nucleus.CellCommandListener;

import org.dcache.util.Args;

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

    synchronized boolean isExpired(JobInfo job, long now)
    {
        long started = job.getStartTime();
        long lastAccessed =
            job instanceof IoJobInfo ?
            ((IoJobInfo)job).getLastTransferred() :
            now;

        return
            ((getLastAccessed() > 0L) && (lastAccessed > 0L) &&
             ((now - lastAccessed) > getLastAccessed())) ||
            ((getTotal() > 0L) && (started > 0L) &&
             ((now - started) > getTotal()));
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

    public synchronized void stop() {
        _worker.interrupt();
    }

    public void addScheduler(String type, IoScheduler scheduler)
    {
        say("Adding scheduler : " + type);
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

    private void say(String str)
    {
        _log.info(str);
    }

    private void esay(String str)
    {
        _log.error(str);
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
                        if (entry.isExpired(info, now)) {
                            esay("Trying to kill <" + entry.getName()
                                 + "> id=" + jobId);
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
