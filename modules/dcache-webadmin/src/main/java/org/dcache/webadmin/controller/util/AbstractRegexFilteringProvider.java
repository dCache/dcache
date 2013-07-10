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
package org.dcache.webadmin.controller.util;

import org.apache.wicket.extensions.markup.html.repeater.util.SortableDataProvider;
import org.apache.wicket.model.IModel;
import org.apache.wicket.model.Model;

import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import org.dcache.util.IRegexFilterable;
import org.dcache.webadmin.view.beans.AbstractRegexFilterBean;

/**
 * Base class for all {@link SortableDataProvider} classes with capabilities for
 * filtering table contents based on string expressions, as provided by
 * {@link IRegexFilterable}.
 * 
 * @author arossi
 */
public abstract class AbstractRegexFilteringProvider<T extends IRegexFilterable>
                extends SortableDataProvider<T> {

    private static final long serialVersionUID = 1L;

    public String getExpression() {
        return getRegexBean().getExpression();
    }

    public int getFlags() {
        return getRegexBean().getFlags();
    }

    public String getFlagsAsString() {
        return getRegexBean().getFlagsAsString();
    }

    public boolean isRegex() {
        return getRegexBean().isRegex();
    }

    @Override
    public Iterator<? extends T> iterator(int first, int count) {
        List<T> data = getFiltered();
        Collections.sort(data, getComparator());
        return data.subList(first, Math.min(first + count, data.size())).iterator();
    }

    @Override
    public IModel<T> model(T object) {
        return Model.of(object);
    }

    public void setExpression(String expression) {
        getRegexBean().setExpression(expression);
    }

    public void setFlags(int flags) {
        getRegexBean().setFlags(flags);
    }

    public void setFlags(String flags) {
        getRegexBean().setFlags(flags);
    }

    public void setRegex(boolean regex) {
        getRegexBean().setRegex(regex);
    }

    @Override
    public int size() {
        return getFiltered().size();
    }

    /**
     * @param entries
     *            assumed to be a thread-local copy, hence not synchronized.
     */
    protected void filterOnExpression(List<T> entries) {
        String expression = getExpression();
        int flags = getFlags();
        if (getExpression() != null) {
            if (isRegex()) {
                try {
                    Pattern pattern = Pattern.compile(expression, flags);
                    for (Iterator<T> it = entries.iterator(); it.hasNext();) {
                        T entry = it.next();
                        if (!pattern.matcher(entry.toFilterableString()).find()) {
                            it.remove();
                        }
                    }
                } catch (PatternSyntaxException e) {
                    entries.clear();
                    throw new IllegalArgumentException(e.getMessage(),
                                    e.getCause());
                }
            } else {
                for (Iterator<T> it = entries.iterator(); it.hasNext();) {
                    T entry = it.next();
                    if (!entry.toFilterableString().contains(expression)) {
                        it.remove();
                    }
                }
            }
        }
    }

    protected abstract Comparator<T> getComparator();

    protected abstract List<T> getFiltered();

    protected abstract AbstractRegexFilterBean<T> getRegexBean();
}
