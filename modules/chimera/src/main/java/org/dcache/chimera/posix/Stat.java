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
package org.dcache.chimera.posix;

import java.sql.Date;
import java.sql.Time;
import java.util.Formatter;

import org.dcache.chimera.UnixPermission;

/**
 *
 * Unix file stat structure abstraction
 *
 */
public class Stat {

    private int _dev; //
    private int _ino; //
    private int _mode; //
    private int _nlink; //
    private int _uid; //
    private int _gid; //
    private int _rdev; //
    private long _size; //
    private long _generation; //

    /*
     * Opposite to classic Unix, all times in milliseconds
     */
    /**
     * Last access time.
     */
    private long _atime;
    /**
     * Last modification time
     */
    private long _mtime;
    /**
     * Last attribute change time.
     */
    private long _ctime;
    /**
     * Creation time.
     */
    private long _crtime;
    private int _blksize = 512; //

    public void setDev(int newDev) {
        _dev = newDev;
    }

    public int getDev() {
        return _dev;
    }

    public void setIno(int newIno) {
        _ino = newIno;
    }

    public int getIno() {
        return _ino;
    }

    public void setMode(int newMode) {
        _mode = newMode;
    }

    public int getMode() {
        return _mode;
    }

    /**
     * Set number of references (link count)
     * @param newMTime
     */
    public void setNlink(int newNlink) {
        _nlink = newNlink;
    }

    public int getNlink() {
        return _nlink;
    }

    public void setUid(int newUid) {
        _uid = newUid;
    }

    public int getUid() {
        return _uid;
    }

    public void setGid(int newGid) {
        _gid = newGid;
    }

    public int getGid() {
        return _gid;
    }

    public void setRdev(int newRdev) {
        _rdev = newRdev;
    }

    public int getRdev() {
        return _rdev;
    }

    public void setSize(long newSize) {
        _size = newSize;
    }

    public long getSize() {
        return _size;
    }

    /**
     * Set change time in milliseconds
     * @param newCTime
     */
    public void setCTime(long newCTime) {
        _ctime = newCTime;
    }

    /**
     *
     * @return change time in milliseconds
     */
    public long getCTime() {
        return _ctime;
    }

    /**
     * Set creation time in milliseconds
     *
     * @param newCrTime
     */
    public void setCrTime(long newCrTime) {
        _crtime = newCrTime;
    }

    /**
     *
     * @return creation time in milliseconds
     */
    public long getCrTime() {
        return _crtime;
    }

    /**
     * Set last accessed time in milliseconds
     * @param newATime
     */
    public void setATime(long newATime) {
        _atime = newATime;
    }

    /**
     *
     * @return last access time in milliseconds
     */
    public long getATime() {
        return _atime;
    }

    /**
     * Set last modification time in milliseconds
     * @param newMTime
     */
    public void setMTime(long newMTime) {
        _mtime = newMTime;
    }

    /**
     *
     * @return last modification time in milliseconds
     */
    public long getMTime() {
        return _mtime;
    }

    public void setBlkSize(int newBlkSize) {
        _blksize = newBlkSize;
    }

    public int getBlkSize() {
        return _blksize;
    }

    public long getBlocks() {
        return (_size + _blksize - 1) / _blksize;

    }

    public long getGeneration() {
        return _generation;
    }

    public void setGeneration(long change) {
        _generation = change;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        try (Formatter formatter = new Formatter(sb)) {
            formatter.format(
                    "%s %8d %6d %6d %6d %6d %s %s",
                    new UnixPermission(this.getMode()),
                    this.getNlink(),
                    this.getUid(),
                    this.getGid(),
                    this.getSize(),
                    this.getGeneration(),
                    new Date(this.getMTime()),
                    new Time(this.getMTime()));
            formatter.flush();
        }

        return sb.toString();
    }

    @Override
    public int hashCode() {
        int hash = 5;
        hash = 79 * hash + this._dev;
        hash = 79 * hash + this._ino;
        hash = 79 * hash + this._mode;
        hash = 79 * hash + this._nlink;
        hash = 79 * hash + this._uid;
        hash = 79 * hash + this._gid;
        hash = 79 * hash + this._rdev;
        hash = 79 * hash + (int) (this._size ^ (this._size >>> 32));
        hash = 79 * hash + (int) (this._atime ^ (this._atime >>> 32));
        hash = 79 * hash + (int) (this._mtime ^ (this._mtime >>> 32));
        hash = 79 * hash + (int) (this._ctime ^ (this._ctime >>> 32));
        hash = 79 * hash + (int) (this._crtime ^ (this._crtime >>> 32));
        hash = 79 * hash + (int) (this._generation ^ (this._generation >>> 32));
        hash = 79 * hash + this._blksize;
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final Stat other = (Stat) obj;
        if (this._dev != other._dev) {
            return false;
        }
        if (this._ino != other._ino) {
            return false;
        }
        if (this._mode != other._mode) {
            return false;
        }
        if (this._nlink != other._nlink) {
            return false;
        }
        if (this._uid != other._uid) {
            return false;
        }
        if (this._gid != other._gid) {
            return false;
        }
        if (this._rdev != other._rdev) {
            return false;
        }
        if (this._size != other._size) {
            return false;
        }
        if (this._atime != other._atime) {
            return false;
        }
        if (this._mtime != other._mtime) {
            return false;
        }
        if (this._ctime != other._ctime) {
            return false;
        }
        if (this._crtime != other._crtime) {
            return false;
        }
        if (this._blksize != other._blksize) {
            return false;
        }
        return true;
    }


    public static void main(String[] args) {

        Stat stat = new Stat();
        stat.setMTime(System.currentTimeMillis());
        System.out.println(stat);

    }
}
