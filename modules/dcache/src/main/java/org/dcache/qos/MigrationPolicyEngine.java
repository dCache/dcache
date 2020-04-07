/*
 * dCache - http://www.dcache.org/
 *
 * Copyright (C) 2017 Deutsches Elektronen-Synchrotron
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.dcache.qos;

import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import diskCacheV111.poolManager.PoolSelectionUnit;
import diskCacheV111.poolManager.PoolSelectionUnit.SelectionPool;
import diskCacheV111.poolManager.StorageUnitInfoExtractor;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.FileNotFoundCacheException;
import diskCacheV111.util.PnfsId;

import dmg.cells.nucleus.CellEndpoint;
import dmg.cells.nucleus.CellPath;
import dmg.cells.nucleus.NoRouteToCellException;

import org.dcache.cells.CellStub;
import org.dcache.pool.migration.PoolMigrationCopyReplicaMessage;
import org.dcache.pool.migration.PoolMigrationMessage;
import org.dcache.pool.repository.ReplicaState;
import org.dcache.pool.repository.StickyRecord;
import org.dcache.poolmanager.PoolMonitor;
import org.dcache.util.CacheExceptionFactory;
import org.dcache.vehicles.FileAttributes;

/**
 *   Handles copying of a file to a pool which is attached to an HSM.
 */
public class MigrationPolicyEngine
{
    private static final Logger LOGGER
                                                 = LoggerFactory.getLogger(
                    MigrationPolicyEngine.class);

    private static Set<String> getPoolsLinkedToStorageUnit(PoolSelectionUnit psu,
                                                           FileAttributes attributes)
    {
        String unitKey = attributes.getStorageClass() + "@" + attributes.getHsm();

        return StorageUnitInfoExtractor.getPoolGroupsFor(unitKey,
                                                         psu,
                                                         false)
                                       .stream()
                                       .map(pgroup -> psu.getPoolsByPoolGroup(pgroup))
                                       .flatMap(c -> c.stream())
                                       .map(SelectionPool::getName)
                                       .collect(Collectors.toSet());
    }

    private class NopHandler extends MigrationCopyCompletionHandler
    {
        @Override
        protected void failure(PnfsId id, Object error) {
            LOGGER.error("{}: QoS migration failure: {}", id, error);
        }

        @Override
        protected void success(PnfsId id) {
            LOGGER.debug("{}: QoS migration success", id);
        }
    }

    private final   FileAttributes                                fileAttributes;
    private final   CellStub                                      cellStub;
    private final   PoolMonitor                                   poolMonitor;
    private final   UUID                                          uuid = UUID.randomUUID();
    private         List<StickyRecord>                            stickyRecords;
    private         ListenableFuture<PoolMigrationMessage>        future;
    private         MigrationCopyCompletionHandler                handler;
    private         boolean                                       cancelled;

    public MigrationPolicyEngine(FileAttributes fileAttributes,
                                 CellStub cellStub,
                                 PoolMonitor poolMonitor)
    {
        this.fileAttributes = fileAttributes;
        this.cellStub = cellStub;
        this.poolMonitor = poolMonitor;

        /*
         *  TODO should not be hard-coded
         */
        this.stickyRecords = Collections.emptyList();
    }

    /*
     *   TODO currently this functionality is limited in a way
     *   TODO that the file can be migrated only to one pool.
     */
    public synchronized void adjust() throws InterruptedException, CacheException,
                    NoRouteToCellException
    {
        if (cancelled) {
            return;
        }

        PoolSelectionUnit psu = poolMonitor.getPoolSelectionUnit();

        Collection<String> sourcePools = fileAttributes.getLocations();

        if (sourcePools.isEmpty()) {
            throw new FileNotFoundCacheException("No source locations found");
        }

        Collection<String> targetPools = getTargetPools(fileAttributes, psu);

        if (targetPools.isEmpty()) {
            throw CacheExceptionFactory.exceptionOf(CacheException.NO_POOL_CONFIGURED,
                                                    "No HSM pool available");
        }

        List<String> samePools = targetPools.stream()
                                            .filter(n -> sourcePools.contains(n))
                                            .collect(Collectors.toList());

        boolean isOnHsmPool = !samePools.isEmpty();

        PnfsId id = fileAttributes.getPnfsId();
        List<URI> locations = fileAttributes.getStorageInfo().locations();

        LOGGER.debug("{}, adjust; locations {}, already on hsm pool? {}: ",
                     id, locations, isOnHsmPool);

        if (handler == null) {
            handler = new NopHandler();
        }

        if (fileAttributes.getStorageInfo().locations().isEmpty()
                        || !isOnHsmPool) {

            String sourcePool = isOnHsmPool ? samePools.get(0) :
                            getRandomPool(sourcePools);

            String target = isOnHsmPool ?
                            samePools.get(0) :
                            getRandomPool(targetPools);

            LOGGER.debug("{}, selected source {}, selected target {}",
                         id, sourcePool, target);

            PoolSelectionUnit.SelectionPool pool = psu.getPool(target);

            LOGGER.debug("{}, target pool {}", id, pool);

            PoolMigrationCopyReplicaMessage message
                            = new PoolMigrationCopyReplicaMessage(uuid,
                                                                  sourcePool,
                                                                  fileAttributes,
                                                                  ReplicaState.PRECIOUS,
                                                                  stickyRecords,
                                                                  false,
                                                                  false,
                                                                  null,
                                                                  false
            );

            LOGGER.debug("{}, sending migration copy replica message to {}.",
                         id, pool.getAddress());
            future = cellStub.send(new CellPath(pool.getAddress()),
                                   message,
                                   CellEndpoint.SendFlag.RETRY_ON_NO_ROUTE_TO_CELL);

            future.addListener(() -> handler.handleReply(future),
                               MoreExecutors.directExecutor());
        } else {
            /**
             *  In this case, we need to guarantee that any external callback
             *  is notified.
             */
            LOGGER.debug("{}, no need to migrate, notifying callback", id);
            handler.success(id);
        }
    }

    public synchronized void cancel()
    {
        cancelled = true;
        if (future != null) {
            future.cancel(true);
        }
    }

    public void setHandler(MigrationCopyCompletionHandler handler)
    {
        this.handler = handler;
    }

    private String getRandomPool(Collection<String> targetPools)
    {
        int locationCount = targetPools.size();
        int r = new Random().nextInt(locationCount);
        Iterator<String> i = targetPools.iterator();

        while (r-- > 0)
        {
            i.next();
        }

        String pool = i.next();
        return pool;
    }

    private Collection<String> getTargetPools(FileAttributes fileAttributes,
                                              PoolSelectionUnit psu)
    {
        Set<String> linkedPools = getPoolsLinkedToStorageUnit(psu, fileAttributes);
        Set<String> hsms = ImmutableSet.of(fileAttributes.getHsm());

        return Arrays.stream(psu.getActivePools())
              .map(psu::getPool)
              .filter(p -> p.hasAnyHsmFrom(hsms))
              .map(SelectionPool::getName)
              .filter(name -> linkedPools.contains(name))
              .collect(Collectors.toSet());
    }
}
