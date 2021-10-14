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

import com.google.common.collect.ImmutableSet;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

/**
 * String conversion untilities for using with Java Regex Patterns. This code has been moved to
 * commons because it is now shared by more than one module.
 *
 * @author arossi
 */
public class RegexUtils {

    public static final String CASE_INSENSITIVE = "CASE_INSENSITIVE";
    public static final String MULTILINE = "MULTILINE";
    public static final String DOTALL = "DOTALL";
    public static final String UNICODE_CASE = "UNICODE_CASE";
    public static final String CANON_EQ = "CANON_EQ";
    public static final String LITERAL = "LITERAL";
    public static final String COMMENTS = "COMMENTS";
    public static final String UNIX_LINES = "UNIX_LINES";

    public static final ImmutableSet<String> FLAG_VALUES
          = new ImmutableSet.Builder<String>()
          .add(CASE_INSENSITIVE)
          .add(MULTILINE)
          .add(DOTALL)
          .add(UNICODE_CASE)
          .add(CANON_EQ)
          .add(LITERAL)
          .add(COMMENTS)
          .add(UNIX_LINES)
          .build();

    /**
     * Translates the string representation of the {@link Pattern} flags into the corresponding Java
     * int value. String can be an or'd set, e.g., "CASE_INSENTIVE | DOTALL".
     *
     * @param flags string representing the or'd flags
     * @return corresponding internal value
     */
    public static int parseFlags(String flags) {
        if (flags == null) {
            return 0;
        }
        int value = 0;
        String[] split = flags.split("[|]");
        for (String s : split) {
            switch (s.trim()) {
                case CASE_INSENSITIVE:
                    value |= Pattern.CASE_INSENSITIVE;
                    break;
                case MULTILINE:
                    value |= Pattern.MULTILINE;
                    break;
                case DOTALL:
                    value |= Pattern.DOTALL;
                    break;
                case UNICODE_CASE:
                    value |= Pattern.UNICODE_CASE;
                    break;
                case CANON_EQ:
                    value |= Pattern.CANON_EQ;
                    break;
                case LITERAL:
                    value |= Pattern.LITERAL;
                    break;
                case COMMENTS:
                    value |= Pattern.COMMENTS;
                    break;
                case UNIX_LINES:
                    value |= Pattern.UNIX_LINES;
                    break;
            }
        }
        return value;
    }

    /**
     * Gives a string representation of the flags value.
     *
     * @param flags internal value
     * @return corresponding string representing the or'd flags
     */
    public static String flagsToString(int flags) {
        StringBuilder result = new StringBuilder();
        List<String> options = new ArrayList<>();

        if ((flags & Pattern.CASE_INSENSITIVE) == Pattern.CASE_INSENSITIVE) {
            options.add(CASE_INSENSITIVE);
        }

        if ((flags & Pattern.MULTILINE) == Pattern.MULTILINE) {
            options.add(MULTILINE);
        }

        if ((flags & Pattern.DOTALL) == Pattern.DOTALL) {
            options.add(UNIX_LINES);
        }

        if ((flags & Pattern.UNICODE_CASE) == Pattern.UNICODE_CASE) {
            options.add(UNICODE_CASE);
        }

        if ((flags & Pattern.CANON_EQ) == Pattern.CANON_EQ) {
            options.add(CANON_EQ);
        }

        if ((flags & Pattern.LITERAL) == Pattern.LITERAL) {
            options.add(LITERAL);
        }

        if ((flags & Pattern.COMMENTS) == Pattern.COMMENTS) {
            options.add(COMMENTS);
        }

        if ((flags & Pattern.UNIX_LINES) == Pattern.UNIX_LINES) {
            options.add(UNIX_LINES);
        }

        Iterator<String> it = options.iterator();

        if (it.hasNext()) {
            result.append(it.next());
        } else {
            return null;
        }

        while (it.hasNext()) {
            result.append(" | ").append(it.next());
        }

        return result.toString();
    }
}
