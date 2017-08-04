package org.dcache.pool.classic;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Required;

import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.Callable;

import diskCacheV111.vehicles.JobInfo;

import dmg.cells.nucleus.CellCommandListener;
import dmg.cells.nucleus.CellInfoProvider;
import dmg.util.command.Command;

import org.dcache.pool.PoolDataBeanProvider;
import org.dcache.pool.classic.json.JobTimeoutManagerData;

public class JobTimeoutManager
        implements Runnable, CellCommandListener, CellInfoProvider,
                PoolDataBeanProvider<JobTimeoutManagerData>
{
    private static final Logger LOGGER =
            LoggerFactory.getLogger(JobTimeoutManager.class);

    private final Thread _worker;

    private IoQueueManager _ioQueues;

    private static String schedulerEntryInfo(MoverRequestScheduler entry) {
        return "(lastAccess=" + (entry.getLastAccessed() / 1000L) +
                        ";total=" + (entry.getTotal() / 1000L) + ")";
    }

    public JobTimeoutManager()
    {
        _worker = new Thread(this, "JobTimeoutManager");
    }

    @Required
    public void setIoQueueManager(IoQueueManager queues)
    {
        _ioQueues = queues;
    }

    public synchronized void start()
    {
        if (_worker.isAlive()) {
            throw new IllegalStateException("Already running");
        }
        _worker.start();
    }

    public synchronized void stop()
    {
        _worker.interrupt();
    }

    @Override
    public void getInfo(PrintWriter pw)
    {
        getDataObject().print(pw);
    }

    @Override
    public JobTimeoutManagerData getDataObject() {
        JobTimeoutManagerData info = new JobTimeoutManagerData();
        info.setLabel("Job Timeout Manager");
        Map<String, String> queueInfo = new HashMap<>();
        _ioQueues.queues().stream().forEach((e) -> queueInfo.put(e.getName(),
                                                                 schedulerEntryInfo(e)));
        info.setQueueInfo(queueInfo);
        return info;
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
        public String call()
                throws IllegalMonitorStateException
        {
            synchronized (JobTimeoutManager.this) {
                JobTimeoutManager.this.notifyAll();
            }
            return "";
        }
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
                for (MoverRequestScheduler jobs : _ioQueues.queues()) {
                    for (JobInfo info : jobs.getJobInfos()) {
                        int jobId = (int) info.getJobId();
                        if (jobs.isExpired(info, now)) {
                            LOGGER.error("Trying to kill <{}> id={}", jobs.getName(), jobId);
                            if (!jobs.cancel(jobId, "killed by JTM")) {
                                throw new NoSuchElementException("Job " + jobId + " not found");
                            }
                        }
                    }
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
