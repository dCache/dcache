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

import java.io.File;
import java.io.IOException;
import java.util.List;

import static org.dcache.chimera.FileSystemProvider.StatCacheOption.NO_STAT;

public class HFile extends File {

    private static final long serialVersionUID = 6304886860060999115L;
    private FsInode _inode;
    private FsInode _parent;
    private final FileSystemProvider _fs;
    private boolean _isNew;

    public HFile(FileSystemProvider fs, String path) {
        super(path);
        _fs = fs;
        try {
            _inode = _fs.path2inode(path);
        } catch (Exception e) {
            // FIXME: actually only is valid exception FileNotFoundHimeraFsException
            _isNew = true;
        }
    }

    public HFile(HFile parent, String name) {
        super(parent, name);

        _parent = parent.getInode();
        _fs = parent.getInode().getFs();
        try {
            _inode = _fs.inodeOf(parent.getInode(), name, NO_STAT);
        } catch (Exception e) {
            // FIXME: actually only is valid exception FileNotFoundHimeraFsException
            _isNew = true;
        }
    }

    @Override
    public boolean exists() {
        return _inode != null;
    }

    @Override
    public boolean isDirectory() {
        return exists() && _inode.isDirectory();
    }

    @Override
    public boolean isFile() {
        return exists() && !_inode.isDirectory();
    }

    @Override
    public String[] list() {

        try {
            List<HimeraDirectoryEntry> fullList = DirectoryStreamHelper.listOf(_inode);
            String[] list = new String[fullList.size()];
            int i = 0;
            for (HimeraDirectoryEntry entry : fullList) {
                list[i++] = entry.getName();
            }
            return list;
        } catch (IOException e) {
            // Ignored
        }
        // according to java.io.File javadoc:
        //
        // Returns null if this abstract pathname
        // does not denote a directory, or if an I/O error occurs.

        return null;
    }

    @Override
    public long length() {
        long size = 0L;
        try {
            if (_inode != null) {
                size = _inode.statCache().getSize();
            }
        } catch (ChimeraFsException e) {
            /*
             * according java.io.File javadoc we have to eat all exceptions
             * and return 0L if the file does not exist or if an I/O error occurs
             */
        }

        return size;
    }

    @Override
    public long lastModified() {
        long mtime = 0L;
        try {
            if (_inode != null) {
                mtime = _inode.statCache().getMTime();
            }
        } catch (ChimeraFsException e) {
            /*
             * according java.io.File javadoc we have to eat all exceptions
             * and return 0L if the file does not exist or if an I/O error occurs
             */
        }

        return mtime;
    }

    @Override
    public boolean createNewFile() throws IOException {
        boolean rc = false;
        if (_isNew) {
            try {
                if (_parent == null) {
                    _parent = _fs.path2inode(super.getParent());
                }

                _inode = _fs.createFile(_parent, super.getName());
                rc = true;
            } catch (ChimeraFsException hfe) {
                throw new IOException(hfe.getMessage());
            }
        }
        return rc;
    }

    @Override
    public boolean mkdir() {
        boolean rc = false;
        if (_isNew) {
            try {
                if (_parent == null) {
                    _parent = _fs.path2inode(super.getParent());
                }
                _inode = _fs.mkdir(_parent, super.getName());
                rc = true;
            } catch (ChimeraFsException hfe) {
                /*
                 * according java.io.File javadoc we have to eat all exceptions
                 * and return false
                 */
            }
        }
        return rc;
    }

    @Override
    public boolean delete() {
        boolean rc = false;
        if (exists()) {
            try {
                _fs.remove(_inode);
            } catch (ChimeraFsException hfe) {
                /*
                 * according java.io.File javadoc we have to eat all exceptions
                 * and return false
                 */
            }
        }
        return rc;
    }

    // Chinera specific
    public FsInode getInode() {
        return _inode;
    }

    public int write(byte[] data) throws IOException {
        return write(0, data, 0, data.length);
    }

    public int write(long pos, byte[] data, int offset, int len) throws IOException {
        return _inode.write(pos, data, offset, len);

    }

    public int read(byte[] data) throws IOException {
        return read(0, data, 0, data.length);
    }

    public int read(long pos, byte[] data, int offset, int len) throws IOException {
        return _inode.read(pos, data, offset, len);
    }

    /* (non-Javadoc)
     * @see java.io.File#equals(java.lang.Object)
     */
    @Override
    public boolean equals(Object obj) {

        if (obj == this) {
            return true;
        }
        if (!(obj instanceof HFile)) {
            return false;
        }

        HFile o = (HFile) obj;

        /*
         * FIXME: check for paths
         */
        return _fs.equals(o._fs);
    }

    /* (non-Javadoc)
     * @see java.io.File#hashCode()
     */
    @Override
    public int hashCode() {
        return 17;
    }
}
