package org.dcache.pool.classic;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Required;

import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import diskCacheV111.vehicles.JobInfo;

import dmg.cells.nucleus.CellCommandListener;
import dmg.cells.nucleus.CellInfoProvider;
import dmg.util.command.Command;

import org.dcache.pool.PoolDataBeanProvider;
import org.dcache.pool.classic.json.JobTimeoutManagerData;
import org.dcache.util.CDCScheduledExecutorServiceDecorator;
import org.dcache.util.FireAndForgetTask;

public class JobTimeoutManager
        implements CellCommandListener, CellInfoProvider,
                PoolDataBeanProvider<JobTimeoutManagerData>
{
    private static final Logger LOGGER =
            LoggerFactory.getLogger(JobTimeoutManager.class);

    private IoQueueManager _ioQueues;

    private final ScheduledExecutorService _scheduler
            = new CDCScheduledExecutorServiceDecorator<>(
                    Executors.newSingleThreadScheduledExecutor(
                            new ThreadFactoryBuilder()
                                    .setNameFormat("JobTimeoutManager")
                                    .build()
                    ));

    private static String schedulerEntryInfo(MoverRequestScheduler entry) {
        return "(lastAccess=" + (entry.getLastAccessed() / 1000L) +
                        ";total=" + (entry.getTotal() / 1000L) + ")";
    }

    @Required
    public void setIoQueueManager(IoQueueManager queues)
    {
        _ioQueues = queues;
    }

    public void start() {
        _scheduler.scheduleWithFixedDelay(new FireAndForgetTask(this::runExpire), 2, 2, TimeUnit.MINUTES);
    }

    public void stop()
    {
        _scheduler.shutdown();
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
            runExpire();
            return "";
        }
    }

    public synchronized void runExpire() {

        long now = System.currentTimeMillis();
        for (MoverRequestScheduler jobs : _ioQueues.queues()) {
            for (JobInfo info : jobs.getJobInfos()) {
                int jobId = (int) info.getJobId();
                if (jobs.isExpired(info, now)) {
                    LOGGER.error("Trying to kill <{}> id={}", jobs.getName(), jobId);
                    if (!jobs.cancel(jobId, "killed by JTM")) {
                        LOGGER.debug("Job <{}> id={} already gone.", jobs.getName(), jobId);
                    }
                }
            }
        }
    }
}
