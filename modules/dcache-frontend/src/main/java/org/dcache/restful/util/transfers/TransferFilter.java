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
package org.dcache.restful.util.transfers;

import com.google.common.base.Strings;

import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

import diskCacheV111.util.TransferInfo;
import diskCacheV111.util.UserInfo;

/**
 * <p>Provides matching based on globbed patterns for strings, and lists or
 *      single Integer/Long values.</p>
 *
 * <p>Maintains a count of the total number of matches found (not thread safe).</p>
 */
public final class TransferFilter {
    private Pattern  door;
    private Pattern  domain;
    private Pattern  prot;
    private Pattern  pnfsId;
    private Pattern  pool;
    private Pattern  host;
    private Pattern  status;
    private Pattern  state;
    private Pattern  vomsGroup;

    private List<Long>    seq;
    private List<Integer> uid;
    private List<Integer> gid;
    private List<Integer> proc;

    private Long     before;
    private Long     after;

    private long     totalMatched = 0L;

    public boolean matches(TransferInfo transferInfo) {
        if (!matches(door, transferInfo.getCellName())) {
            return false;
        }

        if (!matches(domain, transferInfo.getDomainName())) {
            return false;
        }

        if (!matches(prot, transferInfo.getProtocol())) {
            return false;
        }

        if (!matches(pnfsId, transferInfo.getPnfsId())) {
            return false;
        }

        if (!matches(pool, transferInfo.getPool())) {
            return false;
        }

        if (!matches(host, transferInfo.getReplyHost())) {
            return false;
        }

        if (!matches(seq, transferInfo.getSerialId())) {
            return false;
        }

        if (!matches(proc, emptyStringToNullInteger(transferInfo.getProcess()))) {
            return false;
        }

        UserInfo userInfo = transferInfo.getUserInfo();

        if (!matches(uid, userInfo == null ? null :
                        emptyStringToNullInteger(userInfo.getUid()))) {
            return false;
        }

        if (!matches(gid, userInfo == null ? null :
                        emptyStringToNullInteger(userInfo.getGid()))) {
            return false;
        }

        if (!matches(vomsGroup, userInfo == null ? null :
                        userInfo.getPrimaryVOMSGroup())) {
            return false;
        }

        if (!matches(status, transferInfo.getSessionStatus())) {
            return false;
        }

        if (!matches(state, transferInfo.getMoverStatus())) {
            return false;
        }

        Long waiting = transferInfo.getWaitingSince();

        if (before != null && waiting >= before) {
            return false;
        }

        if (after != null && waiting <= after) {
            return false;
        }

        ++totalMatched;

        return true;
    }

    private <T> boolean matches(List<T> list, T value) {
        if (list == null || list.isEmpty()) {
            return true;
        }

        return list.contains(value);
    }

    private boolean matches(Pattern toMatch, String value) {
        if (toMatch == null) {
            return true;
        }
        return toMatch.matcher(value).find();
    }

    public long getTotalMatched() {
        return totalMatched;
    }

    public void resetTotal() {
        totalMatched = 0L;
    }

    public void setAfter(Long after) {
        this.after = after;
    }

    public void setBefore(Long before) {
        this.before = before;
    }

    public void setDomain(List<String> domain) {
        this.domain = compile(domain);
    }

    public void setDoor(List<String> door) {
        this.door = compile(door);
    }

    public void setGid(List<Integer> gid) {
        this.gid = gid;
    }

    public void setHost(List<String> host) {
        this.host = compile(host);
    }

    public void setPnfsId(List<String> pnfsId) {
        this.pnfsId = compile(pnfsId);
    }

    public void setPool(List<String> pool) {
        this.pool = compile(pool);
    }

    public void setProc(List<Integer> proc) {
        this.proc = proc;
    }

    public void setProt(List<String> prot) {
        this.prot = compile(prot);
    }

    public void setSeq(List<Long> seq) {
        this.seq = seq;
    }

    public void setState(String state) {
        if (state != null) {
            this.state = Pattern.compile(state);
        }
    }

    public void setStatus(String status) {
        if (status != null) {
            this.status = Pattern.compile(convertGlobbed(status));
        }
    }

    public void setUid(List<Integer> uid) {
        this.uid = uid;
    }

    public void setVomsGroup(List<String> vomsGroup) {
        this.vomsGroup = compile(vomsGroup);
    }

    private static Pattern compile(List<String> toMatch) {
        if (toMatch == null || toMatch.size() == 0) {
            return null;
        }

        StringBuilder builder = new StringBuilder();

        Iterator<String> iterator = toMatch.iterator();
        builder.append(convertGlobbed(iterator.next()));

        while(iterator.hasNext()) {
            builder.append("|").append(convertGlobbed(iterator.next()));
        }

        return Pattern.compile(builder.toString());
    }

    private static String convertGlobbed(String toMatch) {
        if (toMatch == null) {
            return null;
        }

        return "^" + toMatch.replaceAll("[*]", ".*") + "$";
    }

    private static Integer emptyStringToNullInteger(String value) {
        if ("<unknown>".equals(value)) {
            return null;
        }

        value = Strings.emptyToNull(value);

        return value == null ? null : Integer.parseInt(value);
    }
}
