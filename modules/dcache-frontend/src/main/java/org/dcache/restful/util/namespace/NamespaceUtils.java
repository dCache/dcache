package org.dcache.restful.util.namespace;

import javax.servlet.http.HttpServletRequest;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.FileLocality;
import diskCacheV111.util.PermissionDeniedCacheException;
import diskCacheV111.vehicles.StorageInfo;

import dmg.cells.nucleus.NoRouteToCellException;

import org.dcache.cells.CellStub;
import org.dcache.namespace.FileAttribute;
import org.dcache.namespace.FileType;
import org.dcache.poolmanager.PoolMonitor;
import org.dcache.qos.QoSTransitionEngine;
import org.dcache.qos.QoSTransitionEngine.Qos;
import org.dcache.qos.QoSTransitionEngine.QosStatus;
import org.dcache.restful.providers.JsonFileAttributes;
import org.dcache.restful.util.RequestUser;
import org.dcache.vehicles.FileAttributes;

/**
 * <p>Utilities for obtaining and returning file attributes and qos
 *    information.</p>
 */
public final class NamespaceUtils {
    /**
     * <p>Add quality-of-service attributes (pinned, locality, etc.) </p>
     *
     * @param json               mapped from attributes
     * @param attributes         returned by the query to namespace
     * @param request            to check for client info
     * @param poolMonitor        a PoolMonitor to check locality
     * @param pinmanager         communication with pinmanager
     */
    public static void addQoSAttributes(JsonFileAttributes json,
                                        FileAttributes attributes,
                                        HttpServletRequest request,
                                        PoolMonitor poolMonitor,
                                        CellStub pinmanager)
                    throws CacheException, NoRouteToCellException,
                    InterruptedException {
        if (RequestUser.isAnonymous()) {
            throw new PermissionDeniedCacheException("Permission denied");
        }

        QosStatus status = new QoSTransitionEngine(poolMonitor, pinmanager)
                        .getQosStatus(attributes, request.getRemoteHost());

        json.setCurrentQos(status.getCurrent().displayName());
        Qos target = status.getTarget();
        if (target != null){
            json.setTargetQos(target.displayName());
        }
    }

    /**
     * <p>Map returned attributes to JsonFileAttributes object.</p>
     *
     * @param name                of file
     * @param json                mapped from attributes
     * @param attributes          returned by the query to namespace
     * @param isLocality          used to check weather user queried
     *                            locality of the file
     * @param isLocations         add locations if true
     * @param isOptional          add optional attributes if true
     * @param request             to check for client info
     * @param poolMonitor         for access to remote PoolMonitor
     */
    public static void chimeraToJsonAttributes(String name,
                                               JsonFileAttributes json,
                                               FileAttributes attributes,
                                               boolean isLocality,
                                               boolean isLocations,
                                               boolean isOptional,
                                               HttpServletRequest request,
                                               PoolMonitor poolMonitor) throws CacheException {
        json.setPnfsId(attributes.getPnfsId());

        if (attributes.isDefined(FileAttribute.NLINK)) {
            json.setNlink(attributes.getNlink());
        }

        if (attributes.isDefined(FileAttribute.MODIFICATION_TIME)) {
            json.setMtime(attributes.getModificationTime());
        }

        if (attributes.isDefined(FileAttribute.CREATION_TIME)) {
            json.setCreationTime(attributes.getCreationTime());
        }

        if (attributes.isDefined(FileAttribute.SIZE)) {
            json.setSize(attributes.getSize());
        }

        FileType fileType = null;

        if (attributes.isDefined(FileAttribute.TYPE)) {
            fileType = attributes.getFileType();
            json.setFileType(fileType);
        }

        json.setFileMimeType(name);

        // when user set locality param in the request,
        // the locality should be returned only for directories
        if ((isLocality) && fileType != FileType.DIR) {
            String client = request.getRemoteHost();
            FileLocality fileLocality
                            = poolMonitor.getFileLocality(attributes,
                                                                client);
            json.setFileLocality(fileLocality);
        }

        if (isLocations) {
            if (attributes.isDefined(FileAttribute.LOCATIONS)) {
                json.setLocations(attributes.getLocations());
            }
        }

        if (isOptional) {
            addAllOptionalAttributes(json, attributes);
        }
    }

    /**
     * <p>Adds the rest of the file attributes in the case full
     *    information on the file is requested.</p>
     *
     * @param json                mapped from attributes
     * @param attributes          returned by the query to namespace
     */
    private static void addAllOptionalAttributes(JsonFileAttributes json,
                                                 FileAttributes attributes) {
        if (attributes.isDefined(FileAttribute.ACCESS_LATENCY)) {
            json.setAccessLatency(attributes.getAccessLatency());
        }

        if (attributes.isDefined(FileAttribute.ACL)) {
            json.setAcl(attributes.getAcl());
        }

        if (attributes.isDefined(FileAttribute.ACCESS_TIME)) {
            json.setAtime(attributes.getAccessTime());
        }

        if (attributes.isDefined(FileAttribute.CACHECLASS)) {
            json.setCacheClass(attributes.getCacheClass());
        }

        if (attributes.isDefined(FileAttribute.CHECKSUM)) {
            json.setChecksums(attributes.getChecksums());
        }

        if (attributes.isDefined(FileAttribute.CHANGE_TIME)) {
            json.setCtime(attributes.getChangeTime());
        }

        if (attributes.isDefined(FileAttribute.FLAGS)) {
            json.setFlags(attributes.getFlags());
        }

        if (attributes.isDefined(FileAttribute.OWNER_GROUP)) {
            json.setGroup(attributes.getGroup());
        }

        if (attributes.isDefined(FileAttribute.HSM)) {
            json.setHsm(attributes.getHsm());
        }

        if (attributes.isDefined(FileAttribute.MODE)) {
            json.setMode(attributes.getMode());
        }

        if (attributes.isDefined(FileAttribute.OWNER)) {
            json.setOwner(attributes.getOwner());
        }

        if (attributes.isDefined(FileAttribute.RETENTION_POLICY)) {
            json.setRetentionPolicy(attributes.getRetentionPolicy());
        }

        if (attributes.isDefined(FileAttribute.STORAGECLASS)) {
            json.setStorageClass(attributes.getStorageClass());
        }

        if (attributes.isDefined(FileAttribute.STORAGEINFO)) {
            StorageInfo info = attributes.getStorageInfo();
            json.setStorageInfo(info);
            json.setSuris(info.locations());
        }
    }

    /**
     * Static class, should not be instantiated.
     */
    private NamespaceUtils() {
    }
}
