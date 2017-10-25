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
package org.dcache.util;

import java.io.Serializable;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;

/**
 * <p>Simple abstraction to indicate the name of field to sort
 * and the direction.</p>
 *
 * <p>Constructor supports convention of indicating the sort order by
 *    prefixing '-' to the name in case of reverse.</p>
 */
public class FieldSort implements Serializable {
    private static final long serialVersionUID = -9054869224739638232L;
    private String name;
    private boolean reverse;

    public FieldSort() {
    }

    public FieldSort(String name, boolean reverse) {
        this.name = name;
        this.reverse = reverse;
    }

    public FieldSort(String signPrefixedName) {
        if (signPrefixedName.startsWith("-")) {
            reverse = true;
            name = signPrefixedName.substring(1);
        } else {
            reverse = false;
            name = signPrefixedName;
        }
    }

    public String getName() {
        return name;
    }

    public boolean isReverse() {
        return reverse;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setReverse(boolean reverse) {
        this.reverse = reverse;
    }

    /**
     * <p>Utility which composes a chained comparator
     *      based on an implementation specific Function for getting
     *      the next comparator from a list of FieldSort.</p>
     * @param sort the list of sorting specifications.
     * @param next the function responsible for the comparator based on the field name.
     * @param <T> object to be compared.
     * @return comparator chain respecting the order of the field and direction list.
     */
    public static <T extends Comparable<T>> Comparator<T>
        getSorter(List<FieldSort> sort, Function<FieldSort, Comparator<T>> next) {
        Comparator<T> comparator;

        if (sort == null || sort.isEmpty()) {
            comparator = Comparator.naturalOrder();
        } else {
            Iterator<FieldSort> it = sort.iterator();
            comparator = next.apply(it.next());
            while (it.hasNext()) {
                comparator = comparator.thenComparing(next.apply(it.next()));
            }
        }

        return comparator;
    }

}
