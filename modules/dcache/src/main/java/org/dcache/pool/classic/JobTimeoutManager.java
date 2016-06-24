package org.dcache.pool.classic;

import dmg.cells.nucleus.CellSetupProvider;
import dmg.util.command.Command;
import dmg.util.command.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CopyOnWriteArrayList;

import diskCacheV111.vehicles.IoJobInfo;
import diskCacheV111.vehicles.JobInfo;

import dmg.cells.nucleus.AbstractCellComponent;
import dmg.cells.nucleus.CellCommandListener;

import static com.google.common.base.Preconditions.checkArgument;

class SchedulerEntry
{
    private final String _name;
    private MoverRequestScheduler _scheduler;
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

    synchronized void setScheduler(MoverRequestScheduler scheduler)
    {
        _scheduler = scheduler;
    }

    synchronized MoverRequestScheduler getScheduler()
    {
        return _scheduler;
    }

    synchronized void setLastAccessed(long lastAccessed)
    {
        checkArgument(lastAccessed >= 0L, "The lastAccess timeout must be " +
                "greater than or equal to 0.");
        _lastAccessed = lastAccessed;
    }

    synchronized long getLastAccessed()
    {
        return _lastAccessed;
    }

    synchronized void setTotal(long total)
    {
        checkArgument(total >= 0L, "The total timeout must be " +
                "greater than or equal to 0.");
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
    implements Runnable, CellCommandListener, CellSetupProvider
{
    private static final Logger _log =
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

    public void addScheduler(String type, MoverRequestScheduler scheduler)
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

    @Command(name = "jtm go",
            hint = "kill transfers that have exceeded time limits",
            description = "Immediately kills all transfers that have exceeded the inactivity " +
                    "limits defined for the corresponding queue. Usually the job timeout manager " +
                    "does this automatically every second minute, but with this command such " +
                    "transfers can be killed immediately.")
    public class JtmGoCommand implements Callable<String>
    {
        @Override
        public synchronized String call()
                throws IllegalMonitorStateException
        {
            notifyAll();
            return "";
        }
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

    @Command(name = "jtm set timeout",
            hint = "set transfer inactivity limits",
            description = "Set the transfer timeout for a specified queue or all queues in " +
                    "this pool. The timeout is a time limit after which a job is considered " +
                    "expired and it will be terminated by the job timeout manager. There are " +
                    "two timeout values needed to be set:\n" +
                    "\t1. The last access timeout is the time durations after which a job is " +
                    "deemed expired based on the time since the last block was transferred.\n" +
                    "\t2. The total timeout is the time duration after which a job is deemed " +
                    "expired based on the start time of the job.\n\n" +
                    "One of these two or both timeout duration must be surpassed before a job " +
                    "is terminated.")
    public class JtmSetTimeoutCommand implements Callable<String>
    {
        @Option(name = "queue", valueSpec = "NAME",
                usage = "Specify the queue name. If no queue is specified, " +
                        "the setting will be applied to all queues.")
        String  queueName;

        @Option(name = "lastAccess", valueSpec = "TIMEOUT",
                usage = "Set the lassAccessed timeout limit in seconds.")
        long  lastAccessed;

        @Option(name = "total", valueSpec = "TIMEOUT",
                usage = "Set the total timeout limit in seconds.")
        long  total;

        @Override
        public synchronized String call() throws IllegalArgumentException
        {
            if (queueName == null) {
                for (SchedulerEntry entry: _schedulers) {
                    entry.setLastAccessed(lastAccessed * 1000L);
                    entry.setTotal(total * 1000L);
                }
            } else {
                SchedulerEntry entry = find(queueName);
                if (entry == null)
                {
                    /*
                        This patch (8639) enforce the main function of jtm set timeout
                        command, which is to set the value of the _total and
                        _lastAccessed variables. To avoid a fail start up (which
                        might occur if an exception is thrown) when a user try to
                        set non-existing queue (in a pool batch file), a waring
                        message below will be printed.
                    */
                    return queueName + " queue does not exist. Please create the queue " +
                            "before setting its timeout values.";
                }
                entry.setLastAccessed(lastAccessed * 1000L);
                entry.setTotal(total * 1000L);
            }
            return "";
        }
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
                    MoverRequestScheduler jobs = entry.getScheduler();
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
