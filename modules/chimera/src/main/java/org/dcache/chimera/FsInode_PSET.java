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

import static java.nio.charset.StandardCharsets.UTF_8;

import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.dcache.chimera.posix.Stat;
import org.dcache.util.Checksum;
import org.dcache.util.ChecksumType;

public class FsInode_PSET extends FsInode {

    private static final long DEFAULT_DURATION_IN_SECS = 300;

    private static final String SIZE = "size";
    private static final String IO = "io";
    private static final String ONLN = "bringonline";
    private static final String STG = "stage";
    private static final String PIN = "pin";
    private static final String UNPIN = "unpin";
    private static final String CKS = "checksum";

    private final String[] _args;

    public FsInode_PSET(FileSystemProvider fs, long ino, String[] args) {
        super(fs, ino, FsInodeType.PSET);
        _args = args.clone();
    }

    @Override
    public boolean isDirectory() {
        return false;
    }

    public boolean isChecksum() {
        return CKS.equals(_args[0]);
    }

    @Override
    public int read(long pos, byte[] data, int offset, int len) {
        return -1;
    }

    @Override
    public void setStat(Stat newStat) throws ChimeraFsException {
        if (newStat.isDefined(Stat.StatAttributes.MTIME)) {
            switch (_args[0]) {
                case SIZE:
                    Stat s = new Stat();
                    try {
                        s.setSize(Long.parseLong(_args[1]));
                    } catch (NumberFormatException ignored) {
                        // Bad values ignored
                    }
                    s.setMTime(newStat.getMTime());
                    _fs.setInodeAttributes(this, 0, s);
                    break;
                case IO:
                    _fs.setInodeIo(this, _args[1].equals("on"));
                    break;
                case ONLN:
                case STG:
                case PIN:
                    handlePinRequest();
                    break;
                case UNPIN:
                    _fs.unpin(new FsInode(_fs, ino()));
                    break;
                case CKS:
                    handleSetChecksum();
                    break;
                default:
                    break;
            }
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

        return byteBase(sb.toString().getBytes(UTF_8));
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
            try {
                lifetime = Long.parseLong(_args[1]);
            } catch (NumberFormatException e) {
                throw new InvalidArgumentChimeraException(
                      "Invalid pin duration: " + e.getMessage());
            }
            if (lifetime < 0) {
                throw new InvalidArgumentChimeraException("Negative pin durations are not allowed");
            }
        } else {
            lifetime = DEFAULT_DURATION_IN_SECS;
        }

        if (_args.length > 2) {
            try {
                unit = TimeUnit.valueOf(_args[2]);
            } catch (IllegalArgumentException e) {
                throw new InvalidArgumentChimeraException("Invalid units: " + _args[2]);
            }
        }

        lifetime = unit.toMillis(lifetime);

        if (lifetime == 0) {
            _fs.unpin(new FsInode(_fs, ino()));
        } else {
            _fs.pin(new FsInode(_fs, ino()), lifetime);
        }
    }

    /*
     *  This method allows overwrite only with ROOT access.
     *  It also allows a non-ROOT user to set only one checksum
     *  (type, value) pair.
     */
    private void handleSetChecksum() throws ChimeraFsException {
        if (_args.length != 3) {
            throw new InvalidArgumentChimeraException("incorrect number of arguments.");
        }

        Set<Checksum> checksums = _fs.getInodeChecksums(this);
        ChecksumType type = ChecksumType.getChecksumType(_args[1]);

        try {
            /*
             * The filesystem implementation does not allow
             * overwrite using the 'setInodeChecksum' method, so
             * an explicit deletion is required.
             */
            if (checksums.stream().anyMatch(c -> type.equals(c.getType()))) {
                _fs.removeInodeChecksum(this, type.getType());
            }

            Checksum cks = new Checksum(type, _args[2]);
            _fs.setInodeChecksum(this, type.getType(), cks.getValue());
        } catch (IllegalArgumentException e) {
            throw new InvalidArgumentChimeraException("Invalid checksum: "
                  + _args[2]
                  + "; "
                  + e.getMessage());
        }
    }
}
