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
package org.dcache.restful.services.transfers;

import com.google.common.collect.ImmutableMap;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.PnfsId;
import diskCacheV111.util.TransferInfo;
import diskCacheV111.util.UserInfo;
import org.dcache.auth.FQAN;
import org.dcache.restful.providers.transfers.TransferList;
import org.dcache.restful.util.transfers.TransferCollector;

import static org.dcache.restful.util.transfers.TransferCollectionUtils.transferKey;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class TransferInfoServiceTest {
    static final Logger LOGGER = LoggerFactory.getLogger(
                    TransferInfoServiceTest.class);

    static final Random RANDOM = new Random(System.currentTimeMillis());

    static final String DATAFILE = "org/dcache/restful/services/transfers/transfers.txt";

    static final PnfsId TESTID = new PnfsId("0000E387208AEB3746038A4B66CC6B528C52");

    static final int TESTIDCOUNT = 456;

    class TestTransferCollector extends TransferCollector {
        Map<String, TransferInfo> map;

        @Override
        public Map<String, TransferInfo> collectData()
                        throws InterruptedException {
            return ImmutableMap.copyOf(map);
        }

        @Override
        public void initialize(Long timeout, TimeUnit timeUnit) {
            JsonParser parser = new JsonParser();
            InputStream input = getClass()
                            .getClassLoader()
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
                allTransfers = map.size();
            });
        }

        @Override
        public void shutdown() {
            map.clear();
        }

        void removeRandom(int count) {
            for (int i = 0; i < count; ++i) {
                int currentSize = map.size();
                int index = Math.abs(RANDOM.nextInt() % currentSize);
                String key = new ArrayList<>(map.keySet()).get(index);
                map.remove(key);
            }
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

    private TransferInfoServiceImpl                             service;
    private TestTransferCollector                               collector;
    private int                                                 allTransfers;

    @Before
    public void setUp() {
        collector = new TestTransferCollector();
        service = new TransferInfoServiceImpl() {
            public void run() {
                synchronized (this) {
                    try {
                        update(this.collector.collectData());
                        notify();
                    } catch (InterruptedException e) {
                    }
                }
            }
        };
        service.setTimeout(100);
        service.setTimeoutUnit(TimeUnit.MILLISECONDS);
        service.setMaxCacheSize(2);
        service.setCollector(collector);
        service.configure();
        collector.initialize(100L, TimeUnit.MILLISECONDS);
        service.run();
    }

    @After
    public void shutDown() {
        service.beforeStop();
    }

    @Test
    public void testClientRefresh() {
        TransferList list = service.get(null, null, null, null);
        int terminated = Math.min(799, allTransfers);
        doUpdate(terminated);
        list = service.get(list.getCurrentToken(), null, null, null);
        assertEquals(allTransfers, list.getTransfers().size());
        list = service.get(null, null, null, null);
        assertEquals(allTransfers - terminated, list.getTransfers().size());
    }

    @Test
    public void testGetAll() {
        TransferList result = service.get(null, null, null, null);
        assertEquals(allTransfers, result.getTransfers().size());
    }

    @Test
    public void testGetWithID() {
        TransferList result = service.get(null, null, null, TESTID);
        assertEquals(TESTIDCOUNT, result.getTransfers().size());
    }

    @Test
    public void testPagingSimulation() {
        Set<String> keys = new HashSet<>();

        UUID token = null;
        int lastCount = 0;

        for (int i = 0; i < allTransfers; i += 100) {
            TransferList next = service.get(token, i, 100, null);
            lastCount += next.getTransfers().size();
            for (TransferInfo info : next.getTransfers()) {
                String key = transferKey(info.getCellName(),
                                         info.getSerialId());
                assertFalse(keys.contains(key));
                keys.add(key);
            }
            assertEquals("offset = " + i, lastCount, keys.size());
            token = next.getCurrentToken();
        }

        assertEquals("final", allTransfers, keys.size());
    }

    @Test
    public void testPopulate() throws CacheException {
        TransferList list = service.get(null, 0, 1, null);
        TransferInfo info = list.getTransfers().get(0);
        TransferInfo query = new TransferInfo();
        query.setCellName(info.getCellName());
        query.setSerialId(info.getSerialId());
        TransferInfo populated = service.populate(query);
        LOGGER.debug(info.toFormattedString());
        LOGGER.debug(populated.toFormattedString());
        assertEquals(info.toFormattedString(), populated.toFormattedString());
    }

    private void doUpdate(int remove) {
        collector.removeRandom(remove);
        try {
            service.update(collector.collectData());
        } catch (InterruptedException e) {
        }
    }
}