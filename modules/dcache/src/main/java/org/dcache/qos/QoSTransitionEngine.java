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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Set;

import diskCacheV111.util.AccessLatency;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.FileLocality;
import diskCacheV111.util.FsPath;
import diskCacheV111.util.PnfsHandler;
import diskCacheV111.util.RetentionPolicy;
import diskCacheV111.vehicles.HttpProtocolInfo;

import dmg.cells.nucleus.NoRouteToCellException;

import org.dcache.cells.CellStub;
import org.dcache.namespace.FileAttribute;
import org.dcache.pinmanager.PinManagerCountPinsMessage;
import org.dcache.pinmanager.PinManagerPinMessage;
import org.dcache.pinmanager.PinManagerUnpinMessage;
import org.dcache.pool.classic.ALRPReplicaStatePolicy;
import org.dcache.pool.classic.ReplicaStatePolicy;
import org.dcache.pool.repository.ReplicaState;
import org.dcache.pool.repository.StickyRecord;
import org.dcache.poolmanager.PoolMonitor;
import org.dcache.vehicles.FileAttributes;

import static org.dcache.qos.QoSTransitionEngine.Qos.*;

/**
 *   This is a very rudimentary impplementation of support for QoS transitions.
 *   The code has been extracted from dcache-frontend in order to make it
 *   available to other modules (such as the bulk service).
 */
public class QoSTransitionEngine
{
    public static final Logger LOGGER
                    = LoggerFactory.getLogger(QoSTransitionEngine.class);

    /*
     * FIXME
     *
     * Here the code is assuming the pluggable behaviour of whichever
     * pool a new file lands on.  Currently, pools have a hard-code policy
     * factory (LFSReplicaStatePolicyFactory), which yields two possibilities:
     * VolatileReplicaStatePolicy if lsf is "volatile" or "transient", or
     * ALRPReplicaStatePolicy otherwise.
     *
     * In the following statement, we assume files always land on non-volatile
     * pools.
     */
    private static final ReplicaStatePolicy POOL_POLICY
                    = new ALRPReplicaStatePolicy();

    public static final  String             QOS_PIN_REQUEST_ID = "qos";

    public static boolean isPinnedForQoS(FileAttributes fileAttributes,
                                         CellStub cellStub)
                    throws CacheException, InterruptedException, NoRouteToCellException
    {
        PinManagerCountPinsMessage message =
                        new PinManagerCountPinsMessage(fileAttributes.getPnfsId(),
                                                       QOS_PIN_REQUEST_ID);
        return cellStub.sendAndWait(message).getCount() != 0;
    }

    private static final Set<FileAttribute> QOS_ATTRIBUTES
                    = Collections.unmodifiableSet
                    (EnumSet.of(FileAttribute.PNFSID,
                                FileAttribute.ACCESS_LATENCY,
                                FileAttribute.RETENTION_POLICY));

    public enum Qos
    {
        DISK
                        {
                            public String displayName() { return "disk"; }
                        },
        TAPE
                        {
                            public String displayName() { return "tape"; }
                        },
        DISK_TAPE
                        {
                            public String displayName() { return "disk+tape"; }
                        },
        VOLATILE        {
                            public String displayName() { return "volatile"; }
                        },
        UNAVAILABLE
                        {
                            public String displayName() { return "unavailable"; }
                        };

        public abstract String displayName();

        public static Qos fromDisplayName(String targetString)
        {
            if (targetString.equalsIgnoreCase("disk")) {
                return DISK;
            } else if (targetString.equalsIgnoreCase("tape")) {
                return TAPE;
            } else if (targetString.equalsIgnoreCase("disk+tape")) {
                return DISK_TAPE;
            } else if (targetString.equalsIgnoreCase("volatile")) {
                return VOLATILE;
            } else if (targetString.equalsIgnoreCase("unavailable")) {
                return UNAVAILABLE;
            } else {
                throw new IllegalArgumentException("no such qos type: "
                                                                   + targetString);
            }
        }
    }

    public class QosStatus
    {
        private final Qos current;
        private final Qos target;

        QosStatus(Qos current, Qos target)
        {
            this.current = current;
            this.target = target;
        }

        QosStatus(Qos current)
        {
            this.current = current;
            this.target = null;
        }

        public Qos getCurrent()
        {
            return current;
        }

        public Qos getTarget()
        {
            return target;
        }
    }

    private final CellStub poolManager;
    private final PoolMonitor poolMonitor;
    private final PnfsHandler pnfsHandler;
    private final CellStub pinManager;

    public QoSTransitionEngine(PoolMonitor poolMonitor,
                               CellStub pinManager)
    {
        this(null, poolMonitor, null, pinManager);
    }

    public QoSTransitionEngine(CellStub poolManager,
                               PoolMonitor poolMonitor,
                               PnfsHandler pnfsHandler,
                               CellStub pinManager)
    {
        this.poolManager = poolManager;
        this.poolMonitor = poolMonitor;
        this.pnfsHandler = pnfsHandler;
        this.pinManager = pinManager;
    }

    public void adjustQoS(FsPath path,
                          FileAttributes attributes,
                          String target,
                          String remoteHost)
                    throws  UnsupportedOperationException,
                            URISyntaxException,
                            CacheException,
                            InterruptedException,
                            NoRouteToCellException
    {
        FileLocality locality = getLocality(attributes, remoteHost);

        LOGGER.debug("The Locality of the file: {}", locality);

        if (locality == FileLocality.NONE) {
            throw new UnsupportedOperationException("Transition for directories not supported");
        }

        Qos qosTarget;

        try {
            qosTarget = Qos.fromDisplayName(target);
            LOGGER.debug("New target QoS {}.", qosTarget);
        } catch (IllegalArgumentException e) {
            throw new UnsupportedOperationException("Bad QoS Target type", e);
        }

        AccessLatency currentAccessLatency = attributes.getAccessLatency();
        RetentionPolicy currentRetentionPolicy = attributes.getRetentionPolicy();

        LOGGER.debug("AccessLatency {}, Retention Policy {}.",
                  currentAccessLatency, currentRetentionPolicy);

        FileAttributes modifiedAttr = new FileAttributes();

        switch(qosTarget)
        {
            case DISK_TAPE:
                if (!currentRetentionPolicy.equals(
                                RetentionPolicy.CUSTODIAL)) {
                    modifiedAttr.setRetentionPolicy(
                                    RetentionPolicy.CUSTODIAL);
                    new MigrationPolicyEngine(attributes,
                                              poolManager,
                                              poolMonitor).adjust();
                }

                if (!currentAccessLatency.equals(AccessLatency.ONLINE)) {
                    modifiedAttr.setAccessLatency(AccessLatency.ONLINE);
                }

                // REVISIT when Resilience manages QoS for all files, remove
                if (!isPinnedForQoS(attributes)) {
                    pinForQoS(attributes, remoteHost);
                }

                break;
            case DISK:
                switch (locality) {
                    case ONLINE:
                        /*
                         *  ONLINE locality may not denote ONLINE access latency.
                         *  ONLINE locality and NEARLINE access latency should
                         *  not translate to 'Disk' qos.
                         */
                        if (!currentAccessLatency.equals(AccessLatency.ONLINE)) {
                            modifiedAttr.setAccessLatency(AccessLatency.ONLINE);
                        }

                        // REVISIT when Resilience manages QoS for all files, remove
                        if (!isPinnedForQoS(attributes)) {
                            pinForQoS(attributes, remoteHost);
                        }

                        break;
                    default:
                        if (currentRetentionPolicy.equals(RetentionPolicy.CUSTODIAL)) {
                            /*
                             *  Technically, to make the QoS semantics in
                             *  Chimera consistent, one would need to change this
                             *  to REPLICA, even though this would not trigger
                             *  deletion from tape.  It is probably best to
                             *  continue not supporting this transition.
                             */
                            throw new UnsupportedOperationException("Unsupported QoS transition");
                        }
                        break;
                }
                break;
            case TAPE:
                if (!currentRetentionPolicy.equals(
                                RetentionPolicy.CUSTODIAL)) {
                    modifiedAttr.setRetentionPolicy(
                                    RetentionPolicy.CUSTODIAL);
                    new MigrationPolicyEngine(attributes,
                                              poolManager,
                                              poolMonitor).adjust();
                }

                if (!currentAccessLatency.equals(
                                AccessLatency.NEARLINE)) {
                    modifiedAttr.setAccessLatency(
                                    AccessLatency.NEARLINE);
                }

                // REVISIT when Resilience manages QoS for all files, remove
                unpinForQoS(attributes);

                break;
            default:
                throw new UnsupportedOperationException("Unsupported QoS target for transition");
        }

        if (modifiedAttr.isDefined(FileAttribute.ACCESS_LATENCY) ||
                        modifiedAttr.isDefined(FileAttribute.RETENTION_POLICY)) {
            pnfsHandler.setFileAttributes(path, modifiedAttr, QOS_ATTRIBUTES);
        }
    }

    /*
     * REVISIT when Resilience can handle all file QoS, remove pinned checks
     * REVISIT use of VOLATILE (DOES NOT ACTUALLY MEAN THIS ...)
     */
    public QosStatus getQosStatus(FileAttributes attributes, String remoteHost)
                    throws InterruptedException, CacheException,
                    NoRouteToCellException {
        boolean isPinnedForQoS
                        = QoSTransitionEngine.isPinnedForQoS(attributes, pinManager);
        FileLocality locality = getLocality(attributes, remoteHost);
        AccessLatency currentAccessLatency
                        = attributes.getAccessLatencyIfPresent().orElse(null);
        RetentionPolicy currentRetentionPolicy
                        = attributes.getRetentionPolicyIfPresent().orElse(null);

        boolean policyIsTape = currentRetentionPolicy == RetentionPolicy.CUSTODIAL;
        boolean latencyIsDisk = currentAccessLatency == AccessLatency.ONLINE
                        || isPinnedForQoS;

        switch (locality) {
            case NEARLINE:
                if (policyIsTape) {
                    if (latencyIsDisk) {
                        /*
                         *  In transition.
                         */
                        return new QosStatus(TAPE, DISK_TAPE);
                    }
                    return new QosStatus(TAPE);
                } else {
                    /*
                     * not possible according to present
                     * locality definition of NEARLINE; but eventually,
                     * if this happens, something has happened
                     * to the file (could be a REPLICA NEARLINE
                     * file whose only copy has been removed
                     * from the pool).
                     */
                    return new QosStatus(UNAVAILABLE);
                }
            case ONLINE:
                if (latencyIsDisk) {
                    if (policyIsTape) {
                        /*
                         *  In transition.
                         */
                        return new QosStatus(DISK, DISK_TAPE);
                    }
                    return new QosStatus(DISK);
                } else {
                    /*
                     *  This is the case where we have found a
                     *  cached file.  Since locality here means
                     *  this cannot be CUSTODIAL, and it is not AL ONLINE,
                     *  it must be REPLICA NEARLINE.  What is the QoS?
                     */
                    if (policyIsTape) {
                        /*
                         *  In transition.
                         */
                        return new QosStatus(VOLATILE, TAPE);
                    }
                    return new QosStatus(VOLATILE);
                }
            case ONLINE_AND_NEARLINE:
                if (latencyIsDisk) {
                    return new QosStatus(DISK_TAPE);
                } else {
                    /*
                     *  This is ambiguous.  It could be that the file
                     *  is present on disk, but it is 'unpinned' or has
                     *  an access latency of NEARLINE (a cached replica)
                     *  now, or it could be a transition to a TAPE
                     *  from DISK+TAPE (i.e., is it the current or
                     *  target QoS which is TAPE?).
                     *
                     *  The situation is undecidable. So we leave
                     *  the target Qos blank for the moment.
                     */
                    return new QosStatus(TAPE);
                }
                /*
                 * Transitions away from tape are currently forbidden.
                 */
            case NONE:
                // implies the target is a directory.
                return directoryQoS(attributes);
            case UNAVAILABLE:
                return new QosStatus(UNAVAILABLE);
            case LOST:
                // currently not used by dCache
            default:
                throw new CacheException("Unexpected file locality: " + locality);
        }
    }

    private QosStatus directoryQoS(FileAttributes attributes)
    {
        ReplicaState state = POOL_POLICY.getTargetState(attributes);
        boolean isSticky = POOL_POLICY.getStickyRecords(attributes).stream()
                                      .anyMatch(StickyRecord::isNonExpiring);
        Qos qos;
        if (state == ReplicaState.PRECIOUS) {
            qos =  isSticky ? DISK_TAPE : TAPE;
        } else {
            qos =  isSticky ? DISK : VOLATILE;
        }
        return new QosStatus(qos);
    }

    private FileLocality getLocality(FileAttributes attributes,
                                     String remoteHost)
    {
        return poolMonitor.getFileLocality(attributes, remoteHost);
    }

    private boolean isPinnedForQoS(FileAttributes attributes)
                    throws CacheException,
                    InterruptedException,
                    NoRouteToCellException
    {
        return isPinnedForQoS(attributes, pinManager);
    }

    /**
     *  The QOS_PIN_REQUEST_ID is stored for the pin to allow filtering on files
     *  that are pinned by Qos or SRM.
     */
    private void pinForQoS(FileAttributes attributes,
                           String remoteHost)
                    throws URISyntaxException
    {
        HttpProtocolInfo protocolInfo =
                        new HttpProtocolInfo("Http", 1, 1,
                                             new InetSocketAddress(
                                                             remoteHost,
                                                             0),
                                             null,
                                             null, null,
                                             new URI("http",
                                                     remoteHost,
                                                     null, null));

        PinManagerPinMessage message =
                        new PinManagerPinMessage(attributes,
                                                 protocolInfo,
                                                 QOS_PIN_REQUEST_ID,
                                                 -1);

        pinManager.notify(message);
    }

    /**
     *  Only unpin files stored with the QOS_PIN_REQUEST_ID.
     */
    private void unpinForQoS(FileAttributes attributes)
    {
        PinManagerUnpinMessage message
                        = new PinManagerUnpinMessage(attributes.getPnfsId());
        message.setRequestId(QOS_PIN_REQUEST_ID);
        pinManager.notify(message);
    }
}
