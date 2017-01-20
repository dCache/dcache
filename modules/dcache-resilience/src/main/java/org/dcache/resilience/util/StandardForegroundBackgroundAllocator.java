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

/**
 * <p>The proportion allotted to foreground vs background is based
 *    on the proportion of operations in the queues.</p>
 *
 * <p>The maximum allocation percentage places both lower and upper
 *    bounds on the apportioned number of slots per queue.</p>
 *
 * <p>Weighting is done following these rules:
 *    <ol>
 *        <li>Compute the foreground proportion, and take the minimum
 *            between that and the maximum allowed.</li>
 *        <li>Round the value of the weighted available slots, and
 *            take the minimum between that and the actual foreground
 *            queue size.</li>
 *        <li>Give the minimum between the remaining slots and the
 *            size of the background queue to background.</li>
 *        <li>If there are still slots available after this, give
 *            them to the foreground.</li>
 *        <li>If either background or foreground proportions compute
 *            to 0 using this method, elect one of the waiting tasks
 *            to run from the other quota.  This may still result
 *            in 0 tasks being given to background, if there is only
 *            1 slot available.</li>
 *    </ol>
 * </p>
 */
public class StandardForegroundBackgroundAllocator extends
                ForegroundBackgroundAllocator {

    @Override
    public ForegroundBackgroundAllocation allocate(long slots,
                                                   long occupied,
                                                   long foreground,
                                                   long background,
                                                   double maxAllocation) {
        long available = slots - occupied;

        /*
         * No open slots.
         */
        if (available == 0) {
            return new ForegroundBackgroundAllocation(0L, 0L);
        }

        /*
         * In the case of a single slot available, give it to the foreground.
         */
        if (available == 1) {
            return new ForegroundBackgroundAllocation(1L, 0L);
        }

        /*
         * One of the queues is empty; allocate all slots to the other.
         */
        if (foreground == 0) {
            return new ForegroundBackgroundAllocation(0L, Math.min(available, background));
        } else if (background == 0) {
            return new ForegroundBackgroundAllocation(Math.min(available, foreground), 0L);
        }

        double fgsize = foreground;
        double bgsize = background;

        /*
         *  Either the proportion of foreground requests to total queued,
         *  or the max allocation weight, whichever is less.  The proportion
         *  is given a lower bound equal to the complement of the maximum.
         */
        double fgweight  = Math.min(Math.max(fgsize/(fgsize+bgsize),
                                             1.0-maxAllocation),
                                    maxAllocation);

        /*
         *  The weighted quota, or the size of the foreground queue, if the
         *  latter is less than the allocated size.
         */
        long fgAvailable = Math.min(Math.round(available * fgweight), foreground);

        /*
         *  The remainder of the available slots, or the size of the background
         *  queue, if the latter is less than the allocated size.
         */
        long bgAvailable = Math.min(available - fgAvailable, background);

        /*
         *  In case of under-allocation (fewer background requests than
         *  its allocation), make sure foreground queue gets the remainder.
         */
        fgAvailable = Math.min(available - bgAvailable, foreground);

        /*
         * If the rounding produced a 0 value for available here, the queues
         * are nevertheless > 0, so allocate at least one request to that queue.
         */
        if (fgAvailable == 0) {
            fgAvailable = 1;
            --bgAvailable;
        } else if (bgAvailable == 0) {
            bgAvailable = 1;
            --fgAvailable;
        }

        return new ForegroundBackgroundAllocation(fgAvailable, bgAvailable);
    }
}
