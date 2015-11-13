/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2015 Deutsches Elektronen-Synchrotron
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
package org.dcache.util;

import com.google.common.base.Function;
import com.google.common.collect.Ordering;
import com.google.common.util.concurrent.FutureFallback;
import com.google.common.util.concurrent.ListenableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Stream;

import diskCacheV111.vehicles.IoDoorEntry;
import diskCacheV111.vehicles.IoDoorInfo;
import diskCacheV111.vehicles.IoJobInfo;

import dmg.cells.nucleus.CellPath;
import dmg.cells.services.login.LoginBrokerInfo;
import dmg.cells.services.login.LoginManagerChildrenInfo;

import org.dcache.cells.CellStub;

import static com.google.common.util.concurrent.Futures.*;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

/**
 * Fully asynchronous collector for discovering ongoing transfers.
 *
 * The class uses a terminology aground the following entities:
 *
 *     LoginBrokers -> LoginManagers -> Doors -> Transfers -> Movers
 *
 * These entities are queried in the order shown.
 *
 * The class logs and otherwise ignores all errors.
 */
public class TransferCollector
{
    private static final Logger LOGGER = LoggerFactory.getLogger(TransferCollector.class);

    private final CellStub stub;
    private final Collection<LoginBrokerInfo> doors;

    /**
     * @param stub communication stub
     * @param doors doors to query - if the collection is updated the changes will be reflected in
     *              the data collected by this class
     */
    public TransferCollector(CellStub stub, Collection<LoginBrokerInfo> doors)
    {
        this.stub = stub;
        this.doors = doors;
    }

    public Collection<LoginBrokerInfo> getLoginBrokerInfo()
    {
        return doors;
    }

    public ListenableFuture<Collection<LoginManagerChildrenInfo>> collectLoginManagerInfo(Set<CellPath> loginManagers)
    {
        return transform(query(loginManagers, "get children -binary", LoginManagerChildrenInfo.class,
                               "Failed to query login manager: {}", null),
                         removeNulls());
    }

    public ListenableFuture<Collection<IoDoorInfo>> collectDoorInfo(Set<CellPath> doors)
    {
        return transform(query(doors, "get door info -binary", IoDoorInfo.class,
                               "Failed to query door: ", null),
                         removeNulls());
    }

    public ListenableFuture<Collection<IoJobInfo>> collectMovers(Set<CellPath> pools)
    {
        return transform(query(pools, "mover ls -binary", IoJobInfo[].class,
                               "Failed to query pool: {}", new IoJobInfo[0]),
                         flatten());
    }

    public ListenableFuture<List<Transfer>> collectTransfers()
    {
        Collection<LoginBrokerInfo> loginBrokerInfo =
                getLoginBrokerInfo();
        ListenableFuture<Collection<LoginManagerChildrenInfo>> loginManagerInfo =
                collectLoginManagerInfo(getLoginManagers(loginBrokerInfo));
        ListenableFuture<Collection<IoDoorInfo>> doorInfo =
                transform(loginManagerInfo,
                          (Collection<LoginManagerChildrenInfo> l) -> collectDoorInfo(getDoors(l)));
        return transform(doorInfo,
                         (Collection<IoDoorInfo> l) ->
                                 transform(collectMovers(getPools(l)),
                                           (Collection<IoJobInfo> movers) -> getTransfers(l, movers)));
    }

    public static Set<CellPath> getPools(Collection<IoDoorInfo> movers)
    {
        return movers.stream()
                .map(IoDoorInfo::getIoDoorEntries)
                .flatMap(Collection::stream)
                .map(IoDoorEntry::getPool)
                .filter(name -> name != null && !name.isEmpty() && !name.equals("<unknown>"))
                .map(CellPath::new)
                .collect(toSet());
    }

    public static Set<CellPath> getLoginManagers(Collection<LoginBrokerInfo> loginBrokerInfos)
    {
        return loginBrokerInfos.stream().map(d -> new CellPath(d.getCellName(), d.getDomainName())).collect(toSet());
    }

    public static Set<CellPath> getDoors(Collection<LoginManagerChildrenInfo> loginManagerInfos)
    {
        return loginManagerInfos.stream()
                .flatMap(i -> i.getChildren().stream().map(c -> new CellPath(c, i.getCellDomainName())))
                .collect(toSet());
    }

    public static List<Transfer> getTransfers(Collection<IoDoorInfo> doors, Collection<IoJobInfo> movers)
    {
        Map<String, IoJobInfo> index = createIndex(movers);
        return doors.stream()
                .flatMap(info -> getTransfers(info, index))
                .collect(toList());
    }

    private static Map<String, IoJobInfo> createIndex(Collection<IoJobInfo> movers)
    {
        /* The collection is sorted by job id to ensure that for movers with the
         * same session ID, the one created last is used.
         */
        Map<String, IoJobInfo> index = new HashMap<>();
        for (IoJobInfo info : Ordering.natural().onResultOf(IoJobInfo::getJobId).sortedCopy(movers)) {
            index.put(getKey(info), info);
        }
        return index;
    }

    private static Stream<Transfer> getTransfers(IoDoorInfo door, Map<String, IoJobInfo> movers)
    {
        return door.getIoDoorEntries().stream().map(e -> new Transfer(door, e, movers.get(getKey(door, e))));
    }

    private static String getKey(IoDoorInfo info, IoDoorEntry entry)
    {
        return info.getCellName() + "@" + info.getDomainName() + "#" + entry.getSerialId();
    }

    private static String getKey(IoJobInfo mover)
    {
        return mover.getClientName() + "#" + mover.getClientId();
    }

    private static <T> Function<List<T[]>, List<T>> flatten()
    {
        return l -> l.stream().flatMap(Arrays::stream).collect(toList());
    }

    private static <T> Function<List<T>, List<T>> removeNulls()
    {
        return l -> l.stream().filter(Objects::nonNull).collect(toList());
    }

    private <T> ListenableFuture<List<T>> query(Collection<CellPath> cells, String query, Class<T> returnType,
                                                String errorMsg, T defaultValue)
    {
        List<ListenableFuture<T>> futures =
                cells.stream()
                        .map(cell -> withFallback(stub.send(cell, query, returnType),
                                                  logAndIgnore(errorMsg, defaultValue)))
                        .collect(toList());
        return allAsList(futures);
    }

    private static <T> FutureFallback<T> logAndIgnore(String msg, T defaultValue)
    {
        return t -> {
            LOGGER.debug(msg, t.toString());
            return immediateFuture(defaultValue);
        };
    }

    /**
     * Immutable class to represent all available information about a transfer.
     */
    public static class Transfer
    {
        final IoDoorInfo door;
        final IoDoorEntry session;
        final IoJobInfo mover;

        private Transfer(IoDoorInfo door, IoDoorEntry session, IoJobInfo mover)
        {
            this.door = door;
            this.session = session;
            this.mover = mover;
        }

        public IoDoorInfo door()
        {
            return door;
        }

        public IoDoorEntry session()
        {
            return session;
        }

        @Nullable
        public IoJobInfo mover()
        {
            return mover;
        }
    }

    public static class ByDoorAndSequence implements Comparator<Transfer>
    {
        @Override
        public int compare(Transfer o1, Transfer o2)
        {
            int tmp = o1.door.getDomainName().compareTo(o2.door.getDomainName()) ;
            if (tmp != 0) {
                return tmp;
            }
            tmp = o1.door.getCellName().compareTo(o2.door.getCellName()) ;
            if (tmp != 0) {
                return tmp;
            }
            return Long.compare(o1.session.getSerialId(), o2.session.getSerialId());
        }
    }
}
