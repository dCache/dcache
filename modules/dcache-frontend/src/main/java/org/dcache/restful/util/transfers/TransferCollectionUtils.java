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

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Ordering;

import javax.security.auth.Subject;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import diskCacheV111.util.TransferInfo;
import diskCacheV111.util.UserInfo;
import diskCacheV111.vehicles.IoDoorEntry;
import diskCacheV111.vehicles.IoDoorInfo;
import diskCacheV111.vehicles.IoJobInfo;
import dmg.cells.nucleus.CellPath;
import dmg.cells.services.login.LoginBrokerInfo;
import dmg.cells.services.login.LoginManagerChildrenInfo;

/**
 * <p>Utility class for aiding in the extraction of
 *      information relevant to active transfers.</p>
 */
public final class TransferCollectionUtils {

    public static <T> Function<List<T[]>, List<T>> flatten() {
        return l -> l.stream()
                     .flatMap(Arrays::stream)
                     .collect(Collectors.toList());
    }

    public static Set<CellPath> getDoors(
                    Collection<LoginManagerChildrenInfo> loginManagerInfos) {
        return loginManagerInfos.stream()
                                .flatMap(i -> i.getChildren().stream()
                                               .map(c -> new CellPath(c,
                                                                      i.getCellDomainName())))
                                .collect(Collectors.toSet());
    }

    public static Set<CellPath> getLoginManagers(
                    Collection<LoginBrokerInfo> loginBrokerInfos) {
        return loginBrokerInfos.stream().map(
                        d -> new CellPath(d.getCellName(),
                                          d.getDomainName())).collect(
                        Collectors.toSet());
    }

    public static Set<CellPath> getPools(Collection<IoDoorInfo> movers) {
        return movers.stream()
                     .map(IoDoorInfo::getIoDoorEntries)
                     .flatMap(Collection::stream)
                     .map(IoDoorEntry::getPool)
                     .filter(name -> name != null && !name.isEmpty()
                                     && !name.equals("<unknown>"))
                     .map(CellPath::new)
                     .collect(Collectors.toSet());
    }

    public static <T> Function<List<T>, List<T>> removeNulls() {
        return l -> l.stream()
                     .filter(Objects::nonNull)
                     .collect(Collectors.toList());
    }

    public static String transferKey(String door, Long serialId) {
        Preconditions.checkNotNull(door,
                                   "Transfer key must have a door value.");

        if (serialId == null) {
            return door;
        }

        return door + "-" + serialId;
    }

    public static List<TransferInfo> transfers(Collection<IoDoorInfo> doors,
                                               Collection<IoJobInfo> movers) {
        Map<String, IoJobInfo> index = createIndex(movers);
        return doors.stream()
                    .flatMap(info -> transfers(info, index))
                    .collect(Collectors.toList());
    }

    /**
     * The collection is sorted by job id to ensure that for movers with the
     * same session ID, the one created last is used.
     */
    private static Map<String, IoJobInfo> createIndex(Collection<IoJobInfo> movers) {
        Map<String, IoJobInfo> index = new HashMap<>();
        for (IoJobInfo info : Ordering.natural()
                                      .onResultOf(IoJobInfo::getJobId)
                                      .sortedCopy(movers)) {
            index.put(moverKey(info), info);
        }
        return index;
    }

    private static TransferInfo createTransferInfo(IoDoorInfo door,
                                                   IoDoorEntry session,
                                                   IoJobInfo mover) {
        TransferInfo info = new TransferInfo();
        info.setCellName(door.getCellName());
        info.setDomainName(door.getDomainName());
        info.setSerialId(session.getSerialId());
        info.setProtocol(door.getProtocolFamily(), door.getProtocolVersion());

        Subject subject = session.getSubject();
        if (subject == null) {
            info.setUserInfo(new UserInfo());
        } else {
            info.setSubject(subject);
        }

        info.setProcess(door.getProcess());
        info.setPnfsId(Objects.toString(session.getPnfsId(), ""));
        info.setPool(Objects.toString(session.getPool(), ""));
        info.setReplyHost(Objects.toString(session.getReplyHost(), ""));
        info.setSessionStatus(Objects.toString(session.getStatus(), ""));
        info.setWaitingSince(session.getWaitingSince());

        if (mover != null) {
            info.setMoverId(mover.getJobId());
            info.setMoverStatus(mover.getStatus());
            info.setMoverSubmit(mover.getSubmitTime());
            if (mover.getStartTime() > 0L) {
                info.setTransferTime(mover.getTransferTime());
                info.setBytesTransferred(mover.getBytesTransferred());
                info.setMoverStart(mover.getStartTime());
            }
        }

        return info;
    }

    private static String doorKey(IoDoorInfo info, IoDoorEntry entry) {
        return info.getCellName() + "@" + info.getDomainName() + "#"
                        + entry.getSerialId();
    }

    private static String moverKey(IoJobInfo mover) {
        return mover.getClientName() + "#" + mover.getClientId();
    }

    private static Stream<TransferInfo> transfers(IoDoorInfo door,
                                                  Map<String, IoJobInfo> movers) {
        return door.getIoDoorEntries().stream()
                   .map(e -> createTransferInfo(door, e, movers.get(
                                   doorKey(door, e))));
    }

    private TransferCollectionUtils() {
    }
}
