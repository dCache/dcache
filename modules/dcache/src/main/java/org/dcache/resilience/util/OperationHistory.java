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
package org.dcache.resilience.util;

import com.google.common.collect.EvictingQueue;

import java.util.Iterator;

/**
 * <p>A small circular buffer for preserving the most recent operation
 *      records after they have completed.</p>
 *
 * <p>Capacity is fixed for the duration of the JVM (only configurable
 *      through properties).</p>
 *
 * <p>Also maintains a separate buffer for failed operations.</p>
 */
public class OperationHistory {
    private EvictingQueue<String> history;
    private EvictingQueue<String> errors;
    private int capacity = 1000;

    public void add(String output, boolean failed) {
        if (failed) {
            synchronized (errors) {
                errors.add(output);
            }
        }

        synchronized (history) {
            history.add(output);
        }
    }

    public String ascending(boolean failed) {
        return ascending(failed, capacity);
    }

    public String ascending(boolean failed, int limit) {
        EvictingQueue<String> queue = failed ? errors : history;
        StringBuilder builder = new StringBuilder();
        int index = 0;
        synchronized (queue) {
            for (Iterator<String> it = queue.iterator();
                 it.hasNext() && index++ < limit; ) {
                builder.append(it.next()).append("\n");
            }
        }
        return builder.toString();
    }

    public String descending(boolean failed) {
        return descending(failed, capacity);
    }

    public String descending(boolean failed, int limit) {
        EvictingQueue<String> queue = failed ? errors : history;
        StringBuilder builder = new StringBuilder();
        int index = 0;
        int start = queue.size() - limit;
        synchronized (queue) {
            for (Iterator<String> it = queue.iterator(); it.hasNext(); ) {
                String output = it.next();
                if (index++ >= start) {
                    builder.insert(0, output + "\n");
                }
            }
        }
        return builder.toString();
    }

    public void initialize() {
        history = EvictingQueue.create(capacity);
        errors = EvictingQueue.create(capacity);
    }

    public void setCapacity(int capacity) {
        this.capacity = capacity;
    }

    public void setHistory(EvictingQueue<String> history) {
        this.history = history;
    }
}
