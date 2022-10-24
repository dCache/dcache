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
package org.dcache.mock;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.PnfsId;
import java.io.IOException;
import java.net.URI;
import java.nio.file.OpenOption;
import java.time.Instant;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import org.dcache.pool.repository.CacheEntry;
import org.dcache.pool.repository.ReplicaRecord;
import org.dcache.pool.repository.ReplicaState;
import org.dcache.pool.repository.RepositoryChannel;
import org.dcache.pool.repository.StickyRecord;
import org.dcache.pool.repository.v5.CacheEntryImpl;
import org.dcache.vehicles.FileAttributes;

public class CacheEntryBuilder {

    public static CacheEntryBuilder aCacheEntry() {
        return new CacheEntryBuilder();
    }

    private PnfsId pnfsId;
    private long replicaSize;
    private long creationTime;
    private long lastAccessTime;
    private int linkCount;
    private boolean sticky;

    FileAttributes fileAttributes;
    ReplicaState state;

    final Collection<StickyRecord> stickyRecords = new HashSet<>();

    public CacheEntryBuilder withPnfsId(String pnfsId) {
        this.pnfsId = new PnfsId(pnfsId);
        return this;
    }

    public CacheEntryBuilder withPnfsId(PnfsId pnfsId) {
        this.pnfsId = pnfsId;
        return this;
    }

    public CacheEntryBuilder withFileAttributes(FileAttributes fileAttributes) {
        this.fileAttributes = fileAttributes;
        return this;
    }

    public CacheEntryBuilder withReplicaSize(long replicaSize) {
        this.replicaSize = replicaSize;
        return this;
    }

    public CacheEntryBuilder withCreationTime(Instant creationTime) {
        this.creationTime = creationTime.toEpochMilli();
        return this;
    }

    public CacheEntryBuilder withLastAccessTime(Instant lastAccessTime) {
        this.lastAccessTime = lastAccessTime.toEpochMilli();
        return this;
    }

    public CacheEntryBuilder withLinkCount(int linkCount) {
        this.linkCount = linkCount;
        return this;
    }

    public CacheEntryBuilder withStickyRecord(String owner, long expiration) {
        stickyRecords.add(new StickyRecord(owner, expiration));
        return this;
    }

    public CacheEntryBuilder withState(ReplicaState state) {
        this.state = state;
        return this;
    }

    public CacheEntry build() throws CacheException {
        return new CacheEntryImpl(new ReplicaRecord() {
            @Override
            public PnfsId getPnfsId() {
                return pnfsId;
            }

            @Override
            public long getReplicaSize() {
                return replicaSize;
            }

            @Override
            public FileAttributes getFileAttributes() throws CacheException {
                return fileAttributes;
            }

            @Override
            public ReplicaState getState() {
                return state;
            }

            @Override
            public URI getReplicaUri() {
                return null;
            }

            @Override
            public RepositoryChannel openChannel(Set<? extends OpenOption> mode)
                  throws IOException {
                return null;
            }

            @Override
            public long getCreationTime() {
                return creationTime;
            }

            @Override
            public long getLastAccessTime() {
                return lastAccessTime;
            }

            @Override
            public void setLastAccessTime(long time) throws CacheException {
            }

            @Override
            public int decrementLinkCount() {
                return linkCount;
            }

            @Override
            public int incrementLinkCount() {
                return linkCount;
            }

            @Override
            public int getLinkCount() {
                return linkCount;
            }

            @Override
            public boolean isSticky() {
                return sticky;
            }

            @Override
            public Collection<StickyRecord> removeExpiredStickyFlags() throws CacheException {
                return null;
            }

            @Override
            public Collection<StickyRecord> stickyRecords() {
                return stickyRecords;
            }

            @Override
            public <T> T update(String why, Update<T> update) throws CacheException {
                return null;
            }
        });
    }
}
