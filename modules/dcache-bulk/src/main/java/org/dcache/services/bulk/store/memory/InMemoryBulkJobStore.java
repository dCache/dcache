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
package org.dcache.services.bulk.store.memory;

import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.dcache.services.bulk.BulkJobStorageException;
import org.dcache.services.bulk.job.BulkJob;
import org.dcache.services.bulk.job.BulkJob.State;
import org.dcache.services.bulk.job.BulkJobKey;
import org.dcache.services.bulk.store.BulkJobStore;

/**
 *  Provides quick access to jobs, which are also queued.
 */
public class InMemoryBulkJobStore extends InMemoryStore implements BulkJobStore
{
    private final Map<String, BulkJob>     jobs    = new HashMap<>();

    public void cancelAll(String requestId)
    {
        Predicate<BulkJob> filter = job -> job.getKey().getRequestId()
                                              .equals(requestId);
        write.lock();
        try {
            /*
             *  Avoid concurrent modification on the stream.
             */
            jobs.values()
                .stream()
                .filter(filter)
                .collect(Collectors.toList())
                .stream().forEach(BulkJob::cancel);
        } finally {
            write.unlock();
        }
    }

    public void delete(BulkJobKey key)
    {
        write.lock();
        try {
            jobs.remove(key.toString());
        } finally {
            write.unlock();
        }
    }

    @Override
    public Collection<BulkJob> find(Predicate<BulkJob> filter, Long limit)
    {
        if (limit == null) {
            limit = Long.MAX_VALUE;
        }

        read.lock();
        try {
            return jobs.values().stream()
                       .filter(filter)
                       .limit(limit)
                       .collect(Collectors.toList());
        } finally {
            read.unlock();
        }
    }

    @Override
    public Optional<BulkJob> getJob(BulkJobKey key)
    {
        read.lock();
        try {
            return Optional.ofNullable(jobs.get(key.toString()));
        } finally {
            read.unlock();
        }
    }

    @Override
    public List<BulkJobKey> keys()
    {
        read.lock();
        try {
            return jobs.values()
                       .stream()
                       .map(BulkJob::getKey)
                       .sorted(Comparator.comparing(BulkJobKey::getKey))
                       .collect(Collectors.toList());
        } finally {
            read.unlock();
        }
    }

    @Override
    public void store(BulkJob job)
    {
        write.lock();
        try {
            jobs.put(job.getKey().toString(), job);
        } finally {
            write.unlock();
        }
    }

    @Override
    public void update(BulkJobKey key, State status, Throwable exception)
                    throws BulkJobStorageException
    {
        write.lock();
        try {
            BulkJob job = jobs.get(key.toString());
            if (job == null) {
                throw new BulkJobStorageException("job not found for " + key);
            }
            job.setState(status);
            if (exception != null) {
                job.setErrorObject(exception);
            }
        } finally {
            write.unlock();
        }
    }
}
