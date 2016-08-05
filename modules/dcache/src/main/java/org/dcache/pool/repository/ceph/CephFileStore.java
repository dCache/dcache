/*
 * dCache - http://www.dcache.org/
 *
 * Copyright (C) 2016 Deutsches Elektronen-Synchrotron
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

import diskCacheV111.util.PnfsId;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.attribute.BasicFileAttributeView;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.util.Set;
import java.util.stream.Collectors;

import org.dcache.pool.movers.IoMode;
import org.dcache.pool.repository.FileStore;
import org.dcache.pool.repository.RepositoryChannel;
import org.dcache.rados4j.IoCtx;
import org.dcache.rados4j.Rados;
import org.dcache.rados4j.RadosClusterInfo;
import org.dcache.rados4j.RadosException;
import org.dcache.rados4j.Rbd;
import org.dcache.rados4j.RbdImage;
import org.dcache.rados4j.RbdImageInfo;

/**
 * A CEPH based implementation of {@link FileStore}.
 */
public class CephFileStore implements FileStore {

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
        try(RbdImage image = rbd.openReadOnly(toImageName(id));) {

            final RbdImageInfo imageInfo = image.stat();

            return new BasicFileAttributeView() {
                @Override
                public String name() {
                    return "basic";
                }

                @Override
                public BasicFileAttributes readAttributes() throws IOException {
                    return new BasicFileAttributes() {
                        final private long now = System.currentTimeMillis();
                        @Override
                        public FileTime lastModifiedTime() {
                            return FileTime.fromMillis(now);
                        }

                        @Override
                        public FileTime lastAccessTime() {
                            return FileTime.fromMillis(now);
                        }

                        @Override
                        public FileTime creationTime() {
                            return FileTime.fromMillis(now);
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

                @Override
                public void setTimes(FileTime lastModifiedTime, FileTime lastAccessTime, FileTime createTime) throws IOException {
                    // NOP
                }
            };
        }
    }

    @Override
    public URI create(PnfsId id) throws IOException {
        String imageName = toImageName(id);
        rbd.create(imageName, 0);
        return toUri(imageName);
    }

    @Override
    public void remove(PnfsId id) throws IOException {
        rbd.remove(toImageName(id));
    }

    @Override
    public RepositoryChannel openDataChannel(PnfsId id, IoMode ioMode) throws IOException {
        return new CephRepositoryChannel(rbd, toImageName(id), ioMode.toOpenString());
    }

    @Override
    public Set<PnfsId> index() throws IOException {
        return rbd.list()
                .stream()
                .map(this::toPnfsId)
                .collect(Collectors.toSet());
    }

    @Override
    public long getFreeSpace() throws IOException {
        RadosClusterInfo clusterInfo = rados.statCluster();
        return clusterInfo.kb_avail.get() * 1024;
    }

    @Override
    public long getTotalSpace() throws IOException {
        RadosClusterInfo clusterInfo = rados.statCluster();
        return clusterInfo.kb.get()*1024;
    }

    @Override
    public boolean isOk() {
        try {
            rados.statPool(ctx);
        } catch (RadosException e) {
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
            return new URI("rbd", poolName, imageName, null, null);
        } catch (URISyntaxException e) {
            // we sholud neve get here
            throw new RuntimeException("Faled to build URI", e);
        }
    }
}
