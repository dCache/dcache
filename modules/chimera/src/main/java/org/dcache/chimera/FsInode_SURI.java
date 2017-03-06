/*
COPYRIGHT STATUS:
Dec 1st 2001, Fermi National Accelerator Laboratory (FNAL) documents and
software are sponsored by the U.S. Department of Energy under Contract No.
DE-AC02-76CH03000. Therefore, the U.S. Government retains a  world-wide
non-exclusive, royalty-free license to publish or reproduce these documents
and software for U.S. Government purposes.  All documents and software
available from this server are protected under the U.S. and Foreign
Copyright Laws, and FNAL reserves all rights.

Distribution of the software available from this server is free of
charge subject to the user following the terms of the Fermitools
Software Legal Information.

Redistribution and/or modification of the software shall be accompanied
by the Fermitools Software Legal Information  (including the copyright
notice).

The user is asked to feed back problems, benefits, and/or suggestions
about the software to the Fermilab Software Providers.

Neither the name of Fermilab, the  URA, nor the names of the contributors
may be used to endorse or promote products derived from this software
without specific prior written permission.

DISCLAIMER OF LIABILITY (BSD):

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
"AS IS" AND ANY EXPRESS OR IMPLIED  WARRANTIES, INCLUDING, BUT NOT
LIMITED TO, THE IMPLIED  WARRANTIES OF MERCHANTABILITY AND FITNESS
FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL FERMILAB,
OR THE URA, OR THE U.S. DEPARTMENT of ENERGY, OR CONTRIBUTORS BE LIABLE
FOR  ANY  DIRECT, INDIRECT,  INCIDENTAL, SPECIAL, EXEMPLARY, OR
CONSEQUENTIAL DAMAGES  (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT
OF SUBSTITUTE  GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR
BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY  OF
LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT  OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE  POSSIBILITY OF SUCH DAMAGE.

Liabilities of the Government:

This software is provided by URA, independent from its Prime Contract
with the U.S. Department of Energy. URA is acting independently from
the Government and in its own private capacity and is not acting on
behalf of the U.S. Government, nor as its contractor nor its agent.
Correspondingly, it is understood and agreed that the U.S. Government
has no connection to this software and in no manner whatsoever shall
be liable for nor assume any responsibility or obligation for any claim,
cost, or damages arising out of or resulting from the use of the software
available from this server.

Export Control:

All documents and software available from this server are subject to U.S.
export control laws.  Anyone downloading information from this server is
obligated to secure any necessary Government licenses before exporting
documents or software obtained from this server.
 */
package org.dcache.chimera;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;

import javax.security.auth.Subject;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.AccessController;
import java.util.List;

import org.dcache.auth.Subjects;
import org.dcache.chimera.posix.Stat;

/**
 * <p>Allows reading and writing of location information for type TAPE.</p>
 * <p>Only root is allowed to overwrite or append values. Deletion occurs
 * with setSize(0) is called [open truncate].</p>
 * <p>Input is validated for URI structure.</p>
 */
public class FsInode_SURI extends FsInode {
    private static final String NEWLINE = "\n";

    /**
     * <p>Enforces required URI parts (scheme, host, query).</p>
     */
    @VisibleForTesting
    static void validate(String line) throws URISyntaxException {
        try {
            URI uri = URI.create(line);
            if (uri.getScheme() == null) {
                throw new URISyntaxException(line, "missing scheme part");
            }
            if (uri.getHost() == null) {
                throw new URISyntaxException(line, "missing host part");
            }
            if (uri.getQuery() == null) {
                throw new URISyntaxException(line, "missing query part");
            }
        } catch (IllegalArgumentException e) {
            throw new URISyntaxException(line, e.getMessage());
        }
    }

    private static boolean isRoot() {
        return Subjects.isRoot(
                        Subject.getSubject(AccessController.getContext()));
    }
    
    private List<StorageLocatable> locations;

    public FsInode_SURI(FileSystemProvider fs, long ino) {
        super(fs, ino, FsInodeType.SURI);
    }

    @Override
    public boolean isDirectory() {
        return false;
    }

    @Override
    public boolean isLink() {
        return false;
    }

    @Override
    public int read(long pos, byte[] data, int offset, int len)
                    throws ChimeraFsException {
        String locations = getLocations();

        byte[] b = locations.getBytes();

        if (pos > b.length) {
            return 0;
        }

        int copyLen = Math.min(len, b.length - (int) pos);
        System.arraycopy(b, (int) pos, data, 0, copyLen);

        return copyLen;
    }

    @Override
    public void setStat(Stat predefinedStat) throws ChimeraFsException {
        if (!isEmpty() && !isRoot()) {
            throw new PermissionDeniedChimeraFsException(
                            "overwrite not allowed");
        }

        super.setStat(predefinedStat);
        if (predefinedStat.getSize() == 0) {
            _fs.clearTapeLocations(this);
        }
    }

    @Override
    public Stat stat() throws ChimeraFsException {
        Stat stat = super.stat();
        stat.setSize(getLocations().length());
        stat.setMode((stat.getMode() & 0000777) | UnixPermission.S_IFREG);

        /*
         * invalidate NFS cache
         */
        stat.setMTime(System.currentTimeMillis());
        stat.setGeneration(stat.getGeneration() + 1);
        return stat;
    }

    @Override
    public int write(long pos, byte[] data, int offset, int len)
                    throws ChimeraFsException {
        if (!isEmpty() && !isRoot()) {
            throw new PermissionDeniedChimeraFsException(
                            "overwrite not allowed");
        }

        String input = new String(data, offset, len).trim();

        if (input.isEmpty()) {
            return len;
        }

        String[] lines = input.split(NEWLINE);
        for (String line : lines) {
            /*
             * Validate uri structure. Caller's responsibility
             * to take care of encoding.
             */
            try {
                validate(line);
            } catch (URISyntaxException e) {
                throw new InvalidArgumentChimeraException(e.toString());
            }

            if (!line.isEmpty()) {
                _fs.addInodeLocation(this,
                                     StorageGenericLocation.TAPE,
                                     line);
            }
        }

        return len;
    }

    private String getLocations() throws ChimeraFsException {
        if (isEmpty()) {
            return "";
        }

        return Joiner.on(NEWLINE)
                     .join(locations.stream()
                                    .map(StorageLocatable::location)
                                    .filter((l) -> l.trim().length() != 0)
                                    .toArray())
                        + NEWLINE;
    }

    private boolean isEmpty() throws ChimeraFsException {
        locations = _fs.getInodeLocations(this,
                                          StorageGenericLocation.TAPE);

        return locations == null || locations.isEmpty();
    }
}
