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

import diskCacheV111.util.AccessLatency;
import diskCacheV111.util.RetentionPolicy;
import java.sql.Date;
import java.sql.Time;
import java.util.EnumSet;
import java.util.Formatter;

import org.dcache.chimera.UnixPermission;

/**
 *
 * Unix file stat structure abstraction
 *
 */
public class Stat {

    public enum StatAttributes {
        DEV,
        INO,
        MODE,
        NLINK,
        UID,
        GID,
        RDEV,
        SIZE,
        FILEID,
        GENERATION,
        ATIME,
        MTIME,
        CTIME,
        CRTIME,
        BLK_SIZE,
        ACCESS_LATENCY,
        RETENTION_POLICY
    }

    /**
     * Set of attributes defined in this {@code stat} object.
     */
    private final EnumSet<StatAttributes> _definedAttrs = EnumSet.noneOf(StatAttributes.class);

    private int _dev; //
    private long _ino; //
    private String _id;
    private int _mode; //
    private int _nlink; //
    private int _uid; //
    private int _gid; //
    private int _rdev; //
    private long _size; //
    private long _generation; //
    private int _accessLatency;
    private int _retentionPolicy;

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

    public Stat()
    {
    }

    public Stat(Stat stat)
    {
        _definedAttrs.addAll(stat._definedAttrs);
        _dev = stat._dev;
        _ino = stat._ino;
        _id = stat._id;
        _mode = stat._mode;
        _nlink = stat._nlink;
        _uid = stat._uid;
        _gid = stat._gid;
        _rdev = stat._rdev;
        _size = stat._size;
        _generation = stat._generation;
        _accessLatency = stat._accessLatency;
        _retentionPolicy = stat._retentionPolicy;
        _atime = stat._atime;
        _mtime = stat._mtime;
        _ctime = stat._ctime;
        _crtime = stat._crtime;
    }

    public int getDev() {
        guard(StatAttributes.DEV);
        return _dev;
    }

    public void setDev(int dev) {
        define(StatAttributes.DEV);
        _dev = dev;
    }

    public long getIno() {
        guard(StatAttributes.INO);
        return _ino;
    }

    public void setIno(long ino) {
        define(StatAttributes.INO);
        _ino = ino;
    }

    public String getId() {
        guard(StatAttributes.FILEID);
        return _id;
    }

    public void setId(String id) {
        define(StatAttributes.FILEID);
        _id = id;
    }

    public int getMode() {
        guard(StatAttributes.MODE);
        return _mode;
    }

    public void setMode(int mode) {
        define(StatAttributes.MODE);
        _mode = mode;
    }

    public int getNlink() {
        guard(StatAttributes.NLINK);
        return _nlink;
    }

    public void setNlink(int nlink) {
        define(StatAttributes.NLINK);
        _nlink = nlink;
    }

    public int getUid() {
        guard(StatAttributes.UID);
        return _uid;
    }

    public void setUid(int uid) {
        define(StatAttributes.UID);
        _uid = uid;
    }

    public int getGid() {
        guard(StatAttributes.GID);
        return _gid;
    }

    public void setGid(int gid) {
        define(StatAttributes.GID);
        _gid = gid;
    }

    public int getRdev() {
        guard(StatAttributes.RDEV);
        return _rdev;
    }

    public void setRdev(int rdev) {
        define(StatAttributes.RDEV);
        _rdev = rdev;
    }

    public long getSize() {
        guard(StatAttributes.SIZE);
        return _size;
    }

    public void setSize(long size) {
        define(StatAttributes.SIZE);
        _size = size;
    }

    public long getATime() {
        guard(StatAttributes.ATIME);
        return _atime;
    }

    public void setATime(long atime) {
        define(StatAttributes.ATIME);
        _atime = atime;
    }

    public long getMTime() {
        guard(StatAttributes.MTIME);
        return _mtime;
    }

    public void setMTime(long mtime) {
        define(StatAttributes.MTIME);
        _mtime = mtime;
    }

    public long getCTime() {
        guard(StatAttributes.CTIME);
        return _ctime;
    }

    public void setCTime(long ctime) {
        define(StatAttributes.CTIME);
        _ctime = ctime;
    }

    public long getGeneration() {
        guard(StatAttributes.GENERATION);
        return _generation;
    }

    public void setGeneration(long generation) {
        define(StatAttributes.GENERATION);
        _generation = generation;
    }

    /**
     * Set creation time in milliseconds
     *
     * @param newCrTime
     */
    public void setCrTime(long newCrTime) {
        define(StatAttributes.CRTIME);
        _crtime = newCrTime;
    }

    /**
     *
     * @return creation time in milliseconds
     */
    public long getCrTime() {
        guard(StatAttributes.CRTIME);
        return _crtime;
    }

    public AccessLatency getAccessLatency() {
        guard(StatAttributes.ACCESS_LATENCY);
        return AccessLatency.getAccessLatency(_accessLatency);
    }

    public void setAccessLatency(AccessLatency accessLatency) {
        define(StatAttributes.ACCESS_LATENCY);
        _accessLatency = accessLatency.getId();
    }

    public RetentionPolicy getRetentionPolicy() {
        guard(StatAttributes.RETENTION_POLICY);
        return RetentionPolicy.getRetentionPolicy(_retentionPolicy);
    }

    public void setRetentionPolicy(RetentionPolicy retentionPolicy) {
        define(StatAttributes.RETENTION_POLICY);
        _retentionPolicy = retentionPolicy.getId();
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
        hash = 79 * hash + (int) (this._ino ^ (this._ino >>> 32));
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
        return true;
    }

    /**
     * Check is attribute defined in this {@code stat} object;
     *
     * @param attr attribute to check
     * @return true iff specified attribute is defined in this stat object.
     */
    public boolean isDefined(StatAttributes attr) {
        return _definedAttrs.contains(attr);
    }

    /**
     * Throws IllegalStateException if attribute is not defined.
     */
    private void guard(StatAttributes attr) throws IllegalStateException {
        if (!isDefined(attr)) {
            throw new IllegalStateException("Attribute is not defined: " + attr);
        }
    }

    private void define(StatAttributes attr) throws IllegalStateException {
        _definedAttrs.add(attr);
    }

    /**
     * Returns <tt>true</tt> iff at least one attribute is set.
     * @return <tt>true</tt> iff at least one attribute is set.
     */
    public boolean isDefinedAny() {
        return !_definedAttrs.isEmpty();
    }

    public EnumSet<StatAttributes> getDefinedAttributeses() {
	return EnumSet.copyOf(_definedAttrs);
    }

    /**
     * Update stat with values provided by <tt>other</tt> Stat.
     * @param other to get values from.
     */
    public void update(Stat other) {
        for (Stat.StatAttributes attr : other.getDefinedAttributeses()) {
            switch (attr) {
            case DEV:
                this.setDev(other.getDev());
                break;
            case MODE:
                this.setMode(other.getMode() & UnixPermission.S_PERMS | getMode() & ~UnixPermission.S_PERMS);
                break;
            case NLINK:
                this.setNlink(other.getNlink());
                break;
            case UID:
                this.setUid(other.getUid());
                break;
            case GID:
                this.setGid(other.getGid());
                break;
            case RDEV:
                this.setRdev(other.getRdev());
                break;
            case SIZE:
                this.setSize(other.getSize());
                break;
            case FILEID:
            case INO:
                break;
            case GENERATION:
                this.setGeneration(other.getGeneration());
                break;
            case ATIME:
                this.setATime(other.getATime());
                break;
            case MTIME:
                this.setMTime(other.getMTime());
                break;
            case CTIME:
                this.setCTime(other.getCTime());
                break;
            case CRTIME:
                this.setCrTime(other.getCrTime());
                break;
            case BLK_SIZE:
                break;
            }
        }
    }
}
