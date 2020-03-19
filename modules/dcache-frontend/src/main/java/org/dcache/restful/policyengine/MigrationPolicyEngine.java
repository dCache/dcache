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
package org.dcache.restful.policyengine;

import com.google.common.util.concurrent.MoreExecutors;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.Random;
import java.util.stream.Collectors;

import diskCacheV111.poolManager.PoolSelectionUnit;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.FileNotFoundCacheException;
import diskCacheV111.vehicles.PoolManagerGetPoolsByHsmMessage;

import diskCacheV111.vehicles.PoolManagerPoolInformation;
import dmg.cells.nucleus.CellEndpoint;
import dmg.cells.nucleus.CellPath;
import dmg.cells.nucleus.NoRouteToCellException;

import org.dcache.cells.AbstractMessageCallback;
import org.dcache.cells.CellStub;
import org.dcache.pool.migration.PoolMigrationCopyReplicaMessage;
import org.dcache.pool.repository.ReplicaState;
import org.dcache.pool.repository.StickyRecord;
import org.dcache.poolmanager.PoolMonitor;
import org.dcache.vehicles.FileAttributes;

import static java.util.Arrays.asList;


public class MigrationPolicyEngine {

    private final FileAttributes fileAttributes;
    private final CellStub cellStub;
    private final PoolMonitor poolMonitor;

    private final UUID uuid = UUID.randomUUID();
    private List<StickyRecord> stickyRecords;

    private static final ReplicaState replicaState = ReplicaState.PRECIOUS;


    public MigrationPolicyEngine(FileAttributes fileAttributes, CellStub cellStub, PoolMonitor poolMonitor) {
        this.fileAttributes = fileAttributes;
        this.cellStub = cellStub;
        this.poolMonitor = poolMonitor;

        //TODO should not be hard-coded
        this.stickyRecords = Collections.emptyList();
    }

    private Collection<String> getSourcePools(FileAttributes fileAttributes) {

        Collection<String> pools = fileAttributes.getLocations();

        return pools;
    }

    private Collection<String> getTargetPolls(FileAttributes fileAttributes, CellStub cellStub)
            throws InterruptedException, CacheException, NoRouteToCellException {

        Collection<String> hsms = asList(fileAttributes.getHsm());

        Collection<PoolManagerPoolInformation> targetPools =
                cellStub.sendAndWait(new PoolManagerGetPoolsByHsmMessage(hsms)).getPools();

        return targetPools.stream().map(PoolManagerPoolInformation::getName).collect(Collectors.toSet());
    }

    private String getRandomPool (Collection<String> targetPools){
        int locationCount = targetPools.size();
        int r = new Random().nextInt(locationCount);
        Iterator<String> i = targetPools.iterator();

        while(r-- > 0) {
            i.next();
        }

        String pool = i.next();
        return pool;
    }

    //TODO currently this functionality is limited in a way that the file can be migrated only to one pool.
    public void adjust() throws InterruptedException, CacheException, NoRouteToCellException {
        Collection<String> sourcePools = getSourcePools(fileAttributes);

        if (sourcePools.isEmpty()) {
            throw new FileNotFoundCacheException("No source locations found");
        }

        Collection<String> targetPools = getTargetPolls(fileAttributes, cellStub);

        if (targetPools.isEmpty()) {
            throw new FileNotFoundCacheException("No HSM pool available");
        }

        List<String> samePools = targetPools.stream().filter(n->
                sourcePools.contains(n)).collect(Collectors.toList());

        boolean isOnHsmPool = !samePools.isEmpty();

        if (fileAttributes.getStorageInfo().locations().isEmpty() || !isOnHsmPool) {

            String sourcePool = isOnHsmPool ? samePools.get(0) :
                    getRandomPool(sourcePools);

            String target = isOnHsmPool ? samePools.get(0) : getRandomPool(targetPools);

            PoolSelectionUnit.SelectionPool pool = poolMonitor.getPoolSelectionUnit().getPool(target);
            PoolMigrationCopyReplicaMessage poolMigrationCopyReplicaMessage = new PoolMigrationCopyReplicaMessage(
                    uuid,
                    sourcePool,
                    fileAttributes,
                    replicaState,
                    stickyRecords,
                    false,
                    false,
                    null,
                    false
            );


            CellStub.addCallback(cellStub.send(new CellPath(pool.getAddress()), poolMigrationCopyReplicaMessage,
                    CellEndpoint.SendFlag.RETRY_ON_NO_ROUTE_TO_CELL),
                    new AbstractMessageCallback<PoolMigrationCopyReplicaMessage>() {

                        @Override
                        public void success(PoolMigrationCopyReplicaMessage message) {
                        }

                        @Override
                        public void timeout(String message) {
                        }

                        @Override
                        public void failure(int rc, Object error) {
                        }
                    },
                    MoreExecutors.directExecutor());
        }
    }

}
