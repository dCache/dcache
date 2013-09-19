package org.dcache.srm.scheduler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import org.dcache.srm.request.Job;

/**
 *
 * @author timur
 */
public class SharedMemoryCache {

    private static final Logger _log =
        LoggerFactory.getLogger(SharedMemoryCache.class);

    /**
     * This set is used to keep the Jobs that are in active
     * (Non final) state in memory
     * If the SRM application runs in the Terracotta clustered environment,
     * the Set is used as a root, so all jobs that are added to this
     * cache are shared between all instances of the SRM servers.
     *
     */
   private  final ReentrantReadWriteLock sharedMemoryReadWriteLock =
            new ReentrantReadWriteLock();
    private  final ReadLock sharedMemoryReadLock = sharedMemoryReadWriteLock.readLock();
    private  final WriteLock sharedMemoryWriteLock = sharedMemoryReadWriteLock.writeLock();
    private  Map<Long,Job> sharedMemoryCache =
            new HashMap<>();


    /**
     * Canonicalizes non-final jobs.
     *
     * @throws IllegalStateException if the canonical instance has a different type than {@code job}
     * @return a canonical instance of {@code job}, which may be job itself
     */
    public <T extends Job> T canonicalize(T job)
    {
        sharedMemoryWriteLock.lock();
        try {
            Job other = sharedMemoryCache.get(job.getId());
            if (other != null) {
                if (!job.getClass().isInstance(other)) {
                    throw new IllegalStateException("Conflicting types for request " + job.getId() + ": " + job.getClass() + " and " + other.getClass());
                }
                job = (T) other;
            } else if (!job.getState().isFinalState()) {
                sharedMemoryCache.put(job.getId(), job);
            }
        } finally {
            sharedMemoryWriteLock.unlock();
        }
        return job;
    }

    /**
     * Updates the registration of job in the cache.
     *
     * A post-condition of this method is that job is in the cache if and only if
     * {@code job} is not final.
     *
     * @param job The canonical instance of a job
     * @throws IllegalArgumentException if {@code job} is not the canonical instance
     */
    public <T extends Job> void update(T job)
    {
        sharedMemoryWriteLock.lock();
        try {
            Job other = sharedMemoryCache.get(job.getId());
            if (other != null) {
                if (other != job) {
                    throw new IllegalArgumentException("Duplicate job #" + job.getId());
                }
                if (job.getState().isFinalState()) {
                    sharedMemoryCache.remove(job.getId());
                }
            } else {
                if (!job.getState().isFinalState()) {
                    sharedMemoryCache.put(job.getId(), job);
                }
            }
        } finally {
            sharedMemoryWriteLock.unlock();
        }
    }

    public Job getJob(long jobId) {
        _log.debug("getJob ( "+jobId + " ) ");
       sharedMemoryReadLock.lock();
       try {
            return sharedMemoryCache.get(jobId);
        } finally {
            sharedMemoryReadLock.unlock();
        }

    }

   /**
    * removes all values from the cache
    */
    public void clearCache() {
        sharedMemoryWriteLock.lock();
        try {
            sharedMemoryCache.clear();
        }finally{
            sharedMemoryWriteLock.unlock();
        }
    }

    public <T extends Job> Set<T> getJobs(Class<T> jobType) {
       sharedMemoryReadLock.lock();
       try {
           Set<T> results = new HashSet<>();
           for(Job job: sharedMemoryCache.values()) {
               if(job.getClass().equals(jobType)) {
                   results.add((T)job);
               }
           }
           return results;
        } finally {
            sharedMemoryReadLock.unlock();
        }

    }
}
