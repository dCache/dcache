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

import org.dcache.rados4j.RadosException;
import org.dcache.rados4j.Rbd;
import org.dcache.rados4j.RbdImage;
import java.io.IOException;
import java.io.SyncFailedException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import org.dcache.pool.repository.RepositoryChannel;

/**
 *  CEPH back-ended implementation of {@link RepositoryChannel}.
 *
 *  Uses CEPH's block device image interface to store the data.
 */
public class CephRepositoryChannel implements RepositoryChannel {

    private RbdImage rbdImage;
    private final boolean rdOnly;
    private long size;
    private long offset = 0;

    public CephRepositoryChannel(Rbd rbd, String name, String mode) throws RadosException {
        switch (mode) {
            case "rw":
            case "rwd":
            case "rws":
                // REVISIT: we do not create image here as it already created by CephFileStore.
                //rbd.create(name, 0);
                rbdImage = rbd.open(name);
                rdOnly = false;
                size = 0;
                break;
            case "r":
                rbdImage = rbd.openReadOnly(name);
                rdOnly = true;
                size = rbdImage.stat().obj_size.get();
                break;

            default:
                throw new IllegalArgumentException("Illegal mode: " + mode);
        }
    }

    @Override
    public synchronized long position() throws IOException {
        return offset;
    }

    @Override
    public synchronized RepositoryChannel position(long position) throws IOException {

        if (rdOnly) {
            offset = Math.min(size, position);
        } else {
            offset = position;
            if (offset > size) {
                this.resize(size);
            }
        }
        return this;
    }

    @Override
    public synchronized long size() throws IOException {
        return size;
    }

    @Override
    public synchronized int write(ByteBuffer src, long position) throws IOException {

        if (position + src.remaining() > size) {
            this.resize(position + src.remaining());
        }

        return rbdImage.write(src, position);
    }

    @Override
    public int read(ByteBuffer dst, long position) throws IOException {

        // CEPH can't read beyond image size
        if (position >= size) {
            return -1;
        }

        return rbdImage.read(dst, position);
    }

    private void resize(long size) throws RadosException {
        rbdImage.resize(size);
        this.size = size;
    }

    @Override
    public synchronized RepositoryChannel truncate(long size) throws IOException {

        if (this.size < size) {
            this.resize(size);
        }
        return this;
    }

    @Override
    public void sync() throws SyncFailedException, IOException {
        // NOP
    }

    @Override
    public long transferTo(long position, long count, WritableByteChannel target) throws IOException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public long transferFrom(ReadableByteChannel src, long position, long count) throws IOException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public long write(ByteBuffer[] srcs, int offset, int length) throws IOException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public long write(ByteBuffer[] srcs) throws IOException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public synchronized int write(ByteBuffer src) throws IOException {
        int n = this.write(src, offset);
        offset += n;
        return n;
    }

    @Override
    public synchronized boolean isOpen() {
        return rbdImage != null;
    }

    @Override
    public synchronized void close() throws IOException {
        rbdImage.close();
        rbdImage = null;
    }

    @Override
    public long read(ByteBuffer[] dsts, int offset, int length) throws IOException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public long read(ByteBuffer[] dsts) throws IOException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public synchronized int read(ByteBuffer dst) throws IOException {

        int n = this.read(dst, offset);
        if (n < 0) {
            return n;
        }
        offset += n;
        return n;
    }

}
