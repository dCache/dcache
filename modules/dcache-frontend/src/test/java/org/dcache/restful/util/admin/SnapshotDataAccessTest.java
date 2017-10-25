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
package org.dcache.restful.util.admin;

import com.google.common.base.Strings;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.junit.Before;
import org.junit.Test;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Predicate;

import diskCacheV111.util.TransferInfo;
import diskCacheV111.util.UserInfo;
import org.dcache.auth.FQAN;
import org.dcache.restful.providers.SnapshotList;

import static org.dcache.restful.util.transfers.TransferCollectionUtils.transferKey;
import static org.junit.Assert.*;

public final class SnapshotDataAccessTest {
    static final String DATAFILE = "transfers.json";

    static final int[] TO_REMOVE = { 113, 234, 397, 401 };

    class TestTransferCollector {
        Map<String, TransferInfo> map;

        public void initialize() {
            JsonParser parser = new JsonParser();
            InputStream input = SnapshotDataAccessTest.class
                                .getResourceAsStream(DATAFILE);
            Reader reader = new InputStreamReader(input);
            Object obj = parser.parse(reader);

            map = new HashMap<>();

            JsonArray jsonArray = (JsonArray) obj;
            jsonArray.forEach((e) -> {
                JsonObject jsonTransfer = e.getAsJsonObject();
                TransferInfo transferInfo = new TransferInfo();
                transferInfo.setCellName(
                                getString(jsonTransfer.get("cellName"),
                                          null));
                transferInfo.setDomainName(
                                getString(jsonTransfer.get("domainName"),
                                          null));
                transferInfo.setSerialId(
                                getLong(jsonTransfer.get("serialId"),
                                        null));
                transferInfo.setProtocol(
                                getString(jsonTransfer.get("protocol"),
                                          null));
                transferInfo.setProcess(
                                getString(jsonTransfer.get("process"),
                                          null));
                transferInfo.setPnfsId(
                                getString(jsonTransfer.get("pnfsId"),
                                          null));
                transferInfo.setPool(
                                getString(jsonTransfer.get("pool"), null));
                transferInfo.setReplyHost(
                                getString(jsonTransfer.get("replyHost"),
                                          null));
                transferInfo.setSessionStatus(
                                getString(jsonTransfer.get("sessionStatus"),
                                          null));
                transferInfo.setWaitingSince(
                                getLong(jsonTransfer.get("waitingSince"),
                                        0L));
                transferInfo.setMoverStatus(
                                getString(jsonTransfer.get("moverStatus"),
                                          null));
                transferInfo.setMoverId(
                                getLong(jsonTransfer.get("moverId"), null));
                transferInfo.setMoverSubmit(
                                getLong(jsonTransfer.get("moverSubmit"),
                                        0L));
                transferInfo.setTransferTime(
                                getLong(jsonTransfer.get("transferTime"),
                                        0L));
                transferInfo.setBytesTransferred(
                                getLong(jsonTransfer.get(
                                                "bytesTransferred"),
                                        0L));
                transferInfo.setMoverStart(
                                getLong(jsonTransfer.get("moverStart"),
                                        0L));

                UserInfo userInfo = new UserInfo();
                userInfo.setUsername(
                                getString(jsonTransfer.get("username"),
                                          null));
                userInfo.setUid(getLong(jsonTransfer.get("uid"), null));
                userInfo.setGid(getLong(jsonTransfer.get("gid"), null));
                userInfo.setPrimaryFqan(new FQAN(getString(
                                jsonTransfer.get(
                                                "primaryFqan"),
                                "")));

                transferInfo.setUserInfo(userInfo);

                map.put(transferKey(transferInfo.getCellName(),
                                    transferInfo.getSerialId()),
                        transferInfo);
            });
        }

        private Long getLong(JsonElement element, Long defaultValue) {
            if (element != null && !(element instanceof JsonNull)) {
                return element.getAsLong();
            }
            return defaultValue;
        }

        private String getString(JsonElement element, String defaultValue) {
            if (element != null && !(element instanceof JsonNull)) {
                return element.getAsString();
            }
            return defaultValue;
        }
    }

    private SnapshotDataAccess<String, TransferInfo> snapshotDataAccess;
    private TestTransferCollector                    collector;
    private SnapshotList<TransferInfo>               snapshotList;
    private List<TransferInfo>                       currentList;
    private UUID                                     currentToken;
    private Integer                                  offset;
    private Integer                                  limit;
    private Predicate<TransferInfo>                  filter;
    private Comparator<TransferInfo>                 sorter;
    private TransferInfo                             elementAtIndex;

    @Before
    public void setUp() throws Exception {
        collector = new TestTransferCollector();
        collector.initialize();
        snapshotDataAccess = new SnapshotDataAccess<>();
        snapshotDataAccess.refresh(collector.map);
        filter = (t) -> true;
        sorter = Comparator.naturalOrder();
        setCurrentList();
        currentToken = null;
        offset = null;
        limit = null;

    }

    @Test
    public void shouldReturnEntriesOnlyMatchingPnfsid() throws Exception {
        whenFilterIsSetToPnfsid("0000E387208AEB3746038A4B66CC6B528C52");
        whenAccessIsRead();
        assertThatEachElementHasPnfsid("0000E387208AEB3746038A4B66CC6B528C52");
        assertThatNextOffSetIs(-1);
    }

    @Test
    public void shouldReturnNewTokenWithCurrentListWhenSnaphotIsExpiredButRequestHasNoToken()
                    throws Exception {
        whenOffsetIsSetTo(100);
        whenLimitIsSetTo(100);
        whenAccessIsRead();
        whenElementIsSavedAtIndex(13);
        whenListChanges();
        whenAccessIsRefreshed();
        whenAccessIsRead();
        assertThatReturnedTokenIsNotNull();
        assertThatSizeOfReturnedListIs(100);
        assertThatCurrentOffSetIs(100);
        assertThatNextOffSetIs(200);
        assertThatElementHasChangedAtIndex(13);
    }

    @Test
    public void shouldReturnNoTokenWithEmptyListWhenSnaphotIsExpired()
                    throws Exception {
        whenOffsetIsSetTo(100);
        whenLimitIsSetTo(100);
        whenAccessIsRead();
        whenTokenIsSaved();
        whenAccessIsRefreshed();
        whenAccessIsRead();
        assertThatReturnedTokenIsNull();
        assertThatSizeOfReturnedListIs(0);
        assertThatCurrentOffSetIs(0);
        assertThatNextOffSetIs(-1);
    }

    @Test
    public void shouldReturnPartialListOfEntriesOnlyMatchingPnfsid()
                    throws Exception {
        whenFilterIsSetToPnfsid("0000E387208AEB3746038A4B66CC6B528C52");
        whenOffsetIsSetTo(100);
        whenLimitIsSetTo(10);
        whenAccessIsRead();
        assertThatEachElementHasPnfsid("0000E387208AEB3746038A4B66CC6B528C52");
        assertThatSizeOfReturnedListIs(10);
        assertThatNextOffSetIs(110);
    }

    @Test
    public void shouldReturnSecondPageOfResults() throws Exception {
        whenOffsetIsSetTo(100);
        whenLimitIsSetTo(100);
        whenAccessIsRead();
        assertThatFirstElementIsElement(100);
        assertThatSizeOfReturnedListIs(100);
        assertThatNextOffSetIs(200);
    }

    @Test
    public void shouldReturnShortPageAndNegativeOffsetWhenLastPageIsRequested()
                    throws Exception {
        whenOffsetIsSetTo(1800);
        whenLimitIsSetTo(100);
        whenAccessIsRead();
        assertThatSizeOfReturnedListIs(23);
        assertThatNextOffSetIs(-1);
    }

    private void assertThatCurrentOffSetIs(int i) {
        assertEquals("Returned current offset of snapshot incorrect",
                     i, snapshotList.getCurrentOffset());
    }

    private void assertThatEachElementHasPnfsid(String s) {
        for (TransferInfo info : snapshotList.getItems()) {
            assertEquals("PnfsIds did not match!", s, info.getPnfsId());
        }
    }

    private void assertThatElementHasChangedAtIndex(int i) {
        assertNotEquals("Element at index " + i + " is the same!",
                        elementId(elementAtIndex),
                        elementId(snapshotList.getItems().get(i)));
    }

    private void assertThatFirstElementIsElement(int i) {
        assertEquals("First returned snapshot element is not correct",
                     elementId(currentList.get(i)),
                     elementId(snapshotList.getItems().get(0)));
    }

    private void assertThatNextOffSetIs(int i) {
        assertEquals("Returned next offset of snapshot incorrect",
                     i, snapshotList.getNextOffset());
    }

    private void assertThatSizeOfReturnedListIs(int i) {
        assertEquals("Size of snapshot list is not correct",
                     i,
                     snapshotList.getItems().size());
    }

    private void assertThatReturnedTokenIsNotNull() {
        assertNotNull("Token returned was null!",
                      snapshotList.getCurrentToken());
    }

    private void assertThatReturnedTokenIsNull() {
        assertNull("Token returned was not null!",
                      snapshotList.getCurrentToken());
    }

    private void setCurrentList()
                    throws InvocationTargetException, IllegalAccessException {
        currentList = snapshotDataAccess.getSnapshot(null,
                                                     null,
                                                     null,
                                                        filter ,
                                                        sorter)
                                                        .getItems();
    }

    private void whenAccessIsRead() throws Exception {
        snapshotList = snapshotDataAccess.getSnapshot(currentToken,
                                                      offset,
                                                      limit,
                                                      filter,
                                                      sorter);
    }

    private void whenAccessIsRefreshed()
                    throws InvocationTargetException, IllegalAccessException {
        snapshotDataAccess.refresh(collector.map);
        setCurrentList();
    }

    private void whenElementIsSavedAtIndex(int i) {
        elementAtIndex = snapshotList.getItems().get(i);
    }

    private void whenLimitIsSetTo(int i) {
        limit = i;
    }

    private void whenListChanges() {
        List<String> keys = new ArrayList<>(collector.map.keySet());
        for (int i = 0; i < TO_REMOVE.length; ++i) {
            String key = keys.get(TO_REMOVE[i]);
            collector.map.remove(key);
        }
    }

    private void whenFilterIsSetToPnfsid(final String pnfsid) throws Exception {
         filter = (t) -> Strings.nullToEmpty(t.getPnfsId()).contains(pnfsid);
    }

    private void whenOffsetIsSetTo(int i) {
        offset = i;
    }

    private void whenTokenIsSaved() {
        currentToken = snapshotList.getCurrentToken();
    }

    private String elementId(TransferInfo info) {
        return info.getCellName() + info.getDomainName() + info.getSerialId();
    }
}
