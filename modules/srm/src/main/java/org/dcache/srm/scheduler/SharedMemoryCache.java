/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package org.dcache.srm.scheduler;

import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import java.util.Set;
import java.util.HashSet;
import java.util.Map;
import java.util.HashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
            new HashMap<Long,Job>();

   public  void updateSharedMemoryChache(Job job) {
        if(job == null) {
            return;
        }
        State state = job.getState();
        sharedMemoryWriteLock.lock();
        try {
            boolean cached =sharedMemoryCache.containsKey(job.getId());
            _log.debug("updateSharedMemoryChache for job ="+job.getId()+
                    " state="+state+ " cached ="+cached);
            if(cached  && state.isFinalState()) {
                _log.debug("removing job #"+job.getId() +" from memory cache");
                sharedMemoryCache.remove(job.getId());
            }
            if(!cached && !state.isFinalState()) {
                _log.debug("putting job #"+job.getId() +" to memory cache");
                sharedMemoryCache.put(job.getId(),job);
            }
        } finally {
            sharedMemoryWriteLock.unlock();
        }


    }

    public Job getJob(Long jobId) {
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
           Set<T> results = new HashSet<T>();
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
