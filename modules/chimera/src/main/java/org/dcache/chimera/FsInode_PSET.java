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
import java.util.concurrent.TimeUnit;

import org.dcache.chimera.posix.Stat;

public class FsInode_PSET extends FsInode {
    private static final String SIZE = "size";
    private static final String IO = "io";
    private static final String ONLN = "bringonline";
    private static final String STG = "stage";
    private static final String PIN = "pin";

    private final String[] _args;

    public FsInode_PSET(FileSystemProvider fs, String id, String[] args) {
        super(fs, id, FsInodeType.PSET);
        _args = args.clone();
    }

    @Override
    public boolean isDirectory() {
        return false;
    }

    @Override
    public int read(long pos, byte[] data, int offset, int len) {
        return -1;
    }

    @Override
    public void setATime(long atime) throws ChimeraFsException {
    }

    @Override
    public void setCTime(long ctime) throws ChimeraFsException {
    }

    @Override
    public void setGID(int gid) throws ChimeraFsException {
    }

    @Override
    public void setMode(int mode) throws ChimeraFsException {
    }

    @Override
    public void setMTime(long mtime) throws ChimeraFsException {
        switch(_args[0]) {
            case SIZE:
                try {
                    _fs.setFileSize(this, Long.parseLong(_args[1]));
                } catch (NumberFormatException ignored) {
                    // Bad values ignored
                }
                super.setMTime(mtime);
                break;
            case IO:
                _fs.setInodeIo(this, _args[1].equals("on"));
                break;
            case ONLN:
            case STG:
            case PIN:
                handlePinRequest();
                break;
            default:
                break;
        }
    }

    @Override
    public void setSize(long size) throws ChimeraFsException {
    }

    @Override
    public void setUID(int uid) throws ChimeraFsException {
    }

    @Override
    public void setStat(Stat newStat) {
        try {
            this.setMTime(newStat.getMTime());
        } catch (ChimeraFsException ignored) {
        }
    }

    @Override
    public Stat stat() throws ChimeraFsException {
        Stat ret = super.stat();
        ret.setMode((ret.getMode() & 0000777) | UnixPermission.S_IFREG);
        // size of magic commands always zero
        ret.setSize(0);
        // only root (trusted) allowed to set IO flag
        if (_args[0].equals("io")) {
            ret.setUid(0);
            ret.setGid(0);
        }
        return ret;
    }

    @Override
    public byte[] getIdentifier() {
        StringBuilder sb = new StringBuilder();

        for (String arg : _args) {
            sb.append(arg).append(':');
        }

        return byteBase(sb.toString().getBytes(Charsets.UTF_8));
    }

    @Override
    public int write(long pos, byte[] data, int offset, int len) {
        return -1;
    }

    @Override
    public boolean equals(Object o) {

        if (o == this) {
            return true;
        }

        if (!(o instanceof FsInode_PSET)) {
            return false;
        }

        return super.equals(o) && Arrays.equals(_args, ((FsInode_PSET) o)._args);
    }

    private void handlePinRequest() throws ChimeraFsException {
        long lifetime;
        TimeUnit unit = TimeUnit.SECONDS;
        if (_args.length > 1) {
            lifetime = Long.parseLong(_args[1]);
            if (lifetime < 0) {
                throw new ChimeraFsException("Negative values are not allowed;"
                                + " please use a positive value for pin lifetime,"
                                + " or 0 to unpin the file");
            }
        } else {
            lifetime = 0;
        }

        if (_args.length > 2) {
            unit = TimeUnit.valueOf(_args[2]);
        }
        lifetime = unit.toMillis(lifetime);

        if (lifetime == 0) {
            _fs.unpin(_id);
        } else {
            _fs.pin(_id, lifetime);
        }
    }
}
