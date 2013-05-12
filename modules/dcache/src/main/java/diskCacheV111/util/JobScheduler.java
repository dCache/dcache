package diskCacheV111.util;

import java.util.NoSuchElementException;

public interface JobScheduler
{
    public interface Job
    {
        public long getSubmitTime();

        public long getStartTime();

        public String getStatusString();

        public int getJobId();
    }

    public int add(Queable runnable);

    public void kill(int jobId, boolean force)
            throws NoSuchElementException;

    public String printJobQueue();

    public int getMaxActiveJobs();

    public int getActiveJobs();

    public int getQueueSize();

    public void setMaxActiveJobs(int maxJobs);

    public void shutdown();
}
