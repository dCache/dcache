/*
 * This library is free software; you can redistribute it and/or modify
 * it under the terms of the GNU Library General Public License as
 * published by the Free Software Foundation; either version 2 of the
 * License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Library General Public License for more details.
 *
 * You should have received a copy of the GNU Library General Public
 * License along with this program (see the file COPYING.LIB for more
 * details); if not, write to the Free Software Foundation, Inc.,
 * 675 Mass Ave, Cambridge, MA 02139, USA.
 */
package org.dcache.chimera;

import com.google.common.base.Charsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.dcache.chimera.posix.Stat;

/**
 * This class has been generalized to be able to provide any number of metadata
 * attribute values.  Currently these must be injected at creation.  (The only
 * one currently implemented is file locality.)
 *
 * @author arossi
 */
public class FsInode_PGET extends FsInode {
    private static final String NEWLINE = "\n\r";

    private final Map<String, String> _metadata = new HashMap<>();
    private String _name;

    public FsInode_PGET(FileSystemProvider fs, String id, String[] args) {
        super(fs, id, FsInodeType.PGET);
        if (args.length > 0) {
            _name = args[0];
            for (int i = 1; i < args.length; i++) {
                _metadata.put(args[i], null);
            }
        }
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }

        if (!(o instanceof FsInode_PGET)) {
            return false;
        }

        FsInode_PGET other = (FsInode_PGET)o;

        if ( _name != null && !_name.equals(other._name)) {
            return false;
        }

        return super.equals(o) &&
                        Arrays.equals(_metadata.keySet().toArray(),
                                        other._metadata.keySet().toArray());
    }

    public synchronized String getMetadataFor(String name) {
        return _metadata.get(name);
    }

    public String getName() {
        return _name;
    }

    public synchronized boolean hasMetadataFor(String name) {
        return _metadata.containsKey(name);
    }

    @Override
    public int read(long pos, byte[] data, int offset, int len) {
        byte[] b = metadata().getBytes();

        if (pos > b.length) {
            return 0;
        }

        int copyLen = Math.min(len, b.length - (int) pos);
        System.arraycopy(b, (int) pos, data, 0, copyLen);

        return copyLen;
    }

    public synchronized void setMetadata(String name, String value) {
        _metadata.put(name, value);
    }

    @Override
    public Stat stat() throws ChimeraFsException {
        Stat ret = super.stat();
        ret.setMode((ret.getMode() & 0000777) | UnixPermission.S_IFREG);
        ret.setSize(metadata().length());
        // invalidate NFS cache
        ret.setMTime(System.currentTimeMillis());
        return ret;
    }

    @Override
    public byte[] getIdentifier() {
        StringBuilder sb = new StringBuilder();

        if(_name != null) {
            sb.append(_name).append(':');
        }

        for (String arg : _metadata.keySet()) {
            sb.append(arg).append(':');
        }

        return byteBase(sb.toString().getBytes(Charsets.UTF_8));
    }

    @Override
    public int write(long pos, byte[] data, int offset, int len) {
        return -1;
    }

    private synchronized String metadata() {
        StringBuilder builder = new StringBuilder();
        for (Entry<String, String> entry: _metadata.entrySet()) {
            builder.append(entry.getKey())
                   .append("=")
                   .append(entry.getValue())
                   .append(NEWLINE);
        }
        return builder.toString();
    }
}
