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
package diskCacheV111.util;

import javax.security.auth.Subject;

import java.io.Serializable;
import java.util.Comparator;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import org.dcache.util.InvalidatableItem;
import org.dcache.util.TimeUtils.DurationParser;

import static org.dcache.util.ByteUnit.BYTES;

/**
 * <p>Encapsulates the representation of active transfer data.</p>
 * <p>
 * <p>Extended in both dCache and webadmin modules.</p>
 */
public class TransferInfo implements Comparable<TransferInfo>, InvalidatableItem, Serializable {
    private static final long serialVersionUID = 7303353263666911507L;

    private static final String FORMAT    = "(%s %s %s)(prot %s)"
                    + "(uid %s gid %s vomsgrp %s)"
                    + "(proc %s)(%s)(pool %s)(client %s)"
                    + "(%s)(state %s)(elapsed %s)(transferred %s)(speed %s)(path %s)\n";

    protected static String getTimeString(long time, boolean display) {
        if (!display) {
            return String.valueOf(time);
        }

        DurationParser durations = new DurationParser(time,
                                                      TimeUnit.MILLISECONDS).parseAll();

        return String.format("%d+%02d:%02d:%02d",
                             durations.get(TimeUnit.DAYS),
                             durations.get(TimeUnit.HOURS),
                             durations.get(TimeUnit.MINUTES),
                             durations.get(TimeUnit.SECONDS));
    }

    public enum MoverState {
        NOTFOUND, STAGING, QUEUED, RUNNING, CANCELED, DONE
    }

    public enum TransferField {
        DOMAIN, PROT, UID, GID, VOMSGROUP, PROC, PNFSID,
        POOL, HOST, STATUS, STATE, WAITING, MOVER
    }

    protected String cellName   = "";
    protected String domainName = "";
    protected Long serialId;
    protected String protocol      = "<unknown>";
    protected String process       = "<unknown>";
    protected String pnfsId        = "";
    protected String path          = "";
    protected String     pool          = "";
    protected String     replyHost     = "";
    protected String     sessionStatus = "";
    protected long       waitingSince;
    protected MoverState moverStatus   = MoverState.NOTFOUND;
    protected Long       transferTime;
    protected Long       bytesTransferred;
    protected Long       moverId;
    protected Long       moverSubmit;
    protected Long       moverStart;
    protected Subject    subject;
    protected UserInfo   userInfo;
    protected boolean    valid         = true;

    @Override
    public int compareTo(TransferInfo o) {
        return Comparator.comparing(TransferInfo::getCellName)
                         .thenComparing(TransferInfo::getDomainName)
                         .thenComparing(TransferInfo::getSerialId)
                         .compare(this, o);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (!(o instanceof TransferInfo)) {
            return false;
        }

        TransferInfo info = (TransferInfo)o;

        return Objects.equals(cellName, info.cellName) &&
                        Objects.equals(domainName, info.domainName) &&
                        Objects.equals(serialId, info.serialId) &&
                        Objects.equals(protocol, info.protocol) &&
                        Objects.equals(pnfsId, info.pnfsId) &&
                        Objects.equals(replyHost, info.replyHost);
    }

    @Override
    public int hashCode() {
        return Objects.hash(cellName,
                            domainName,
                            serialId,
                            protocol,
                            pnfsId,
                            replyHost);
    }

    public Long getBytesTransferred() {
        return bytesTransferred;
    }

    public String getCellName() {
        return cellName;
    }

    public String getDomainName() {
        return domainName;
    }

    public Long getMoverId() {
        return moverId;
    }

    public Long getMoverStart() {
        return moverStart;
    }

    public String getMoverStatus() {
        return moverStatus.name();
    }

    public Long getMoverSubmit() {
        return moverSubmit;
    }

    public String getPath() {
        return path;
    }

    public String getPnfsId() {
        return pnfsId;
    }

    public String getPool() {
        return pool;
    }

    public String getProcess() {
        return process;
    }

    public String getProtocol() {
        return protocol;
    }

    public String getReplyHost() {
        return replyHost;
    }

    public Long getSerialId() {
        return serialId;
    }

    public String getSessionStatus() {
        return sessionStatus;
    }

    public Subject getSubject() {
        return subject;
    }

    public Long getTransferRate() {
        if (bytesTransferred == null) {
            return 0L;
        }

        if (transferTime == null) {
            return 0L;
        }

        long secs = transferTime / 1000;
        return secs > 0.0 ? BYTES.toMiB(bytesTransferred) / secs : 0L;
    }

    public Long getTransferTime() {
        return transferTime;
    }

    public UserInfo getUserInfo() {
        return userInfo;
    }

    public String getUid() {
        return userInfo == null ? null : userInfo.getUid();
    }

    public String getGid() {
        return userInfo == null ? null : userInfo.getGid();
    }

    public String getVomsGroup() {
        return userInfo == null ? null : userInfo.getPrimaryVOMSGroup();
    }

    public long getWaitingSince() {
        return waitingSince;
    }

    public String getTimeWaiting() {
        return timeWaiting(System.currentTimeMillis(),true);
    }

    @Override
    public void invalidate() {
        valid = false;
    }

    @Override
    public boolean isValid() {
        return valid;
    }

    public void setBytesTransferred(Long bytesTransferred) {
        this.bytesTransferred = bytesTransferred;
    }

    public void setCellName(String cellName) {
        this.cellName = cellName;
    }

    public void setDomainName(String domainName) {
        this.domainName = domainName;
    }

    public void setMoverId(Long moverId) {
        this.moverId = moverId;
    }

    public void setMoverStart(Long moverStart) {
        this.moverStart = moverStart;
    }

    public void setMoverStatus(String moverStatus) {
        if (moverStatus == null) {
            this.moverStatus = MoverState.NOTFOUND;
            return;
        }

        this.moverStatus = MoverState.valueOf(moverStatus.toUpperCase());
    }

    public void setMoverSubmit(Long moverSubmit) {
        this.moverSubmit = moverSubmit;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public void setPnfsId(String pnfsId) {
        this.pnfsId = pnfsId;
    }

    public void setPool(String pool) {
        this.pool = pool;
    }

    public void setProcess(String process) {
        this.process = process;
    }

    public void setProtocol(String protocol) {
        this.protocol = protocol;
    }

    public void setProtocol(String protocolFamily, String protocolVersion) {
        this.protocol = protocolFamily + "-" + protocolVersion;
    }

    public void setReplyHost(String replyHost) {
        this.replyHost = replyHost;
    }

    public void setSerialId(Long serialId) {
        this.serialId = serialId;
    }

    public void setSessionStatus(String sessionStatus) {
        this.sessionStatus = sessionStatus;
    }

    public void setSubject(Subject subject) {
        this.subject = subject;
        userInfo = new UserInfo(subject);
    }

    public void setTransferTime(Long transferTime) {
        this.transferTime = transferTime;
    }

    public void setUserInfo(UserInfo userInfo) {
        this.userInfo = userInfo;
    }

    public void setWaitingSince(long waitingSince) {
        this.waitingSince = waitingSince;
    }

    public String toFormattedString() {
        String state = "";
        String size = "";
        String speed = "";

        if (moverStatus != null) {
            state = moverStatus.name();
            if (bytesTransferred != null) {
                size = String.valueOf(bytesTransferred);
            }
            speed = String.valueOf(getTransferRate());
        }

        return String.format(FORMAT,
                             cellName,
                             domainName,
                             String.valueOf(serialId),
                             protocol,
                             userInfo == null ? "" : userInfo.getUid(),
                             userInfo == null ? "" : userInfo.getGid(),
                             userInfo == null ? "" : userInfo.getPrimaryVOMSGroup(),
                             process,
                             pnfsId,
                             pool,
                             replyHost,
                             sessionStatus,
                             state,
                             getTimeWaiting(),
                             size,
                             speed,
                             path);
    }

    protected String timeRunning(long now, boolean display) {
        if (moverStart == null) {
            return "unknown";
        }
        return getTimeString(now - moverStart, display);
    }

    protected String timeElapsedSinceSubmitted(long now, boolean display) {
        if (moverSubmit == null) {
            return "unknown";
        }
        return getTimeString(now - moverSubmit, display);
    }

    protected String timeWaiting(long now, boolean display) {
        return getTimeString(now - waitingSince, display);
    }
}