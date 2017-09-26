/*
 * dCache - http://www.dcache.org/
 *
 * Copyright (C) 2016 - 2017 Deutsches Elektronen-Synchrotron
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
package org.dcache.pool.repository.ceph;

import com.google.common.primitives.Longs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import diskCacheV111.util.PnfsId;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.NoSuchFileException;
import java.nio.file.OpenOption;
import java.nio.file.attribute.BasicFileAttributeView;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.util.Set;
import java.util.stream.Collectors;
import jnr.constants.platform.Errno;

import org.dcache.pool.repository.FileStore;
import org.dcache.pool.repository.RepositoryChannel;
import org.dcache.rados4j.IoCtx;
import org.dcache.rados4j.Rados;
import org.dcache.rados4j.RadosClusterInfo;
import org.dcache.rados4j.RadosException;
import org.dcache.rados4j.Rbd;
import org.dcache.rados4j.RbdImage;
import org.dcache.rados4j.RbdImageInfo;

import static org.dcache.util.ByteUnit.KiB;

/**
 * A CEPH based implementation of {@link FileStore}.
 */
public class CephFileStore implements FileStore {

    protected static final Logger LOGGER = LoggerFactory.getLogger(CephFileStore.class);

    /**
     * RADOS objects extended attribute name to store creation time.
     */
    private final static String CREATION_TIME_ATTR = "creation_time";

    /**
     * RADOS objects extended attribute name to store last access time.
     */
    private final static String LAST_ACCESS_TIME_ATTR = "last_access_time";

    /**
     * RADOS objects extended attribute name to store last modification time.
     */
    private final static String LAST_MODIFICATION_TIME_ATTR = "last_modification_time";

    private final Rados rados;
    private final IoCtx ctx;
    private final Rbd rbd;
    private final String poolName;

    public CephFileStore(String poolName, String cluster, String config) throws RadosException {

        rados = new Rados(cluster, config);
        rados.connect();

        ctx = rados.createIoContext(poolName);
        rbd = ctx.createRbd();
        this.poolName = poolName;
    }

    @Override
    public URI get(PnfsId id) {
        return toUri(toImageName(id));
    }

    @Override
    public boolean contains(PnfsId id) {
        try {
            RbdImage image = rbd.openReadOnly(toImageName(id));
            image.close();
            return true;
        } catch (RadosException e) {
            return false;
        }
    }

    @Override
    public BasicFileAttributeView getFileAttributeView(PnfsId id) throws IOException {
        String imageName = toImageName(id);
        try(RbdImage image = rbd.openReadOnly(imageName)) {

            final RbdImageInfo imageInfo = image.stat();

            return new BasicFileAttributeView() {
                @Override
                public String name() {
                    return "basic";
                }

                @Override
                public BasicFileAttributes readAttributes() throws IOException {
                    return new BasicFileAttributes() {

                        private FileTime getTimeFromXattr(String image, String attr) {
                            long time;
                            try {
                                byte[] b = new byte[Long.BYTES];
                                ctx.getXattr(toObjName(image), attr, b);
                                time = Longs.fromByteArray(b);
                            } catch (RadosException e) {
                                time = 0;
                            }
                            return FileTime.fromMillis(time);
                        }

                        @Override
                        public FileTime lastModifiedTime() {
                            return getTimeFromXattr(imageName, LAST_MODIFICATION_TIME_ATTR);
                        }

                        @Override
                        public FileTime lastAccessTime() {
                            return getTimeFromXattr(imageName, LAST_ACCESS_TIME_ATTR);
                        }

                        @Override
                        public FileTime creationTime() {
                            return getTimeFromXattr(imageName, CREATION_TIME_ATTR);
                        }

                        @Override
                        public boolean isRegularFile() {
                            return true;
                        }

                        @Override
                        public boolean isDirectory() {
                            return false;
                        }

                        @Override
                        public boolean isSymbolicLink() {
                            return false;
                        }

                        @Override
                        public boolean isOther() {
                            return false;
                        }

                        @Override
                        public long size() {
                            return imageInfo.obj_size.longValue();
                        }

                        @Override
                        public Object fileKey() {
                            return null;
                        }
                    };
                }

                private void setTimeToXattr(String image, String attr, FileTime time) throws RadosException {
                    ctx.setXattr(toObjName(image),
                            attr,
                            Longs.toByteArray(time.toMillis()));
                }

                @Override
                public void setTimes(FileTime lastModifiedTime, FileTime lastAccessTime, FileTime createTime) throws IOException {

                    if (lastModifiedTime != null) {
                        setTimeToXattr(imageName, LAST_MODIFICATION_TIME_ATTR, lastModifiedTime);
                    }

                    if (lastAccessTime != null) {
                        setTimeToXattr(imageName, LAST_ACCESS_TIME_ATTR, lastAccessTime);
                    }

                    if (createTime != null) {
                        setTimeToXattr(imageName, CREATION_TIME_ATTR, createTime);
                    }

                }
            };
        } catch (RadosException e) {
            throwIfMappable(e, "Failed to get file's attribute: " + imageName);
            throw e;
        }
    }

    @Override
    public URI create(PnfsId id) throws IOException {
        String imageName = toImageName(id);
        try {
            rbd.create(imageName, 0);
            ctx.setXattr(toObjName(imageName),
                    CREATION_TIME_ATTR,
                    Longs.toByteArray(System.currentTimeMillis()));
        } catch (RadosException e) {
            throwIfMappable(e, "Failed to create file: " + imageName);
            throw e;
        }
        return toUri(imageName);
    }

    @Override
    public void remove(PnfsId id) throws IOException {
        String imageName = toImageName(id);
        try {
            rbd.remove(imageName);
        } catch (RadosException e) {

            // ignore file-not-found error code (negative number).
            if (Errno.valueOf(Math.abs(e.getErrorCode())) == Errno.ENOENT) {
                return;
            }

            throwIfMappable(e, "Failed to remove file: " + imageName);
            throw e;
        }
    }

    @Override
    public RepositoryChannel openDataChannel(PnfsId id, Set<? extends OpenOption> ioMode) throws IOException {
        String imageName = toImageName(id);
        try {
            return new CephRepositoryChannel(rbd, imageName, ioMode);
        } catch (RadosException e) {
            throwIfMappable(e, "Failed to open file: " + imageName);
            throw e;
        }
    }

    @Override
    public Set<PnfsId> index() throws IOException {
        try {
            return rbd.list()
                    .stream()
                    .map(this::toPnfsId)
                    .collect(Collectors.toSet());
        } catch (RadosException e) {
            throwIfMappable(e, "Failed to get list of images");
            throw e;
        }
    }

    @Override
    public long getFreeSpace() throws IOException {
        try {
            RadosClusterInfo clusterInfo = rados.statCluster();
            return KiB.toBytes(clusterInfo.kb_avail.get());
        } catch (RadosException e) {
            throwIfMappable(e, "Failed to get cluster info");
            throw e;
        }
    }

    @Override
    public long getTotalSpace() throws IOException {
        try {
            RadosClusterInfo clusterInfo = rados.statCluster();
            return KiB.toBytes(clusterInfo.kb.get());
        } catch (RadosException e) {
            throwIfMappable(e, "Failed to get cluster info");
            throw e;
        }
    }

    @Override
    public boolean isOk() {
        try {
            rados.statPool(ctx);
        } catch (RadosException e) {
            LOGGER.error("Repository health check failed: {}", e.toString());
            return false;
        }
        return true;
    }

    private String toImageName(PnfsId id) {
        return id.toString();
    }

    private PnfsId toPnfsId(String s) {
        return new PnfsId(s);
    }

    /**
     * Returns object name corresponding to specified RBD image.
     * @param image name.
     */
    private String toObjName(String img) {
        return img + ".rbd";
    }

    private URI toUri(String imageName) {
        try {
            return new URI("rbd", poolName, "/" + imageName, null, null);
        } catch (URISyntaxException e) {
            // we should never get here
            throw new RuntimeException("Failed to build URI", e);
        }
    }

    public void shutdown() throws RadosException {
        try {
            ctx.destroy();
        } finally {
            rados.shutdown();
        }
    }

    private void throwIfMappable(RadosException e, String msg) throws IOException {
        // try to map CEPH errors to dCache/java.io alternatives
        // the errcodes are negative numbers :)

        Errno err = Errno.valueOf(Math.abs(e.getErrorCode()));

        switch(err) {
            case ENOENT:
                throw new NoSuchFileException(msg + " : " + e.getMessage());
        }
    }
}
