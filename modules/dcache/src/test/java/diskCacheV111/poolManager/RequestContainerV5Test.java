/*
 * dCache - http://www.dcache.org/
 *
 * Copyright (C) 2021 Deutsches Elektronen-Synchrotron
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package diskCacheV111.poolManager;

import com.google.common.util.concurrent.SettableFuture;
import diskCacheV111.poolManager.RequestContainerV5.RequestState;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.DestinationCostException;
import diskCacheV111.util.FileNotInCacheException;
import diskCacheV111.util.PermissionDeniedCacheException;
import diskCacheV111.util.PnfsHandler;
import diskCacheV111.util.PnfsId;
import diskCacheV111.util.SourceCostException;
import diskCacheV111.vehicles.InfoMessage;
import diskCacheV111.vehicles.Pool2PoolTransferMsg;
import diskCacheV111.vehicles.PoolFetchFileMessage;
import diskCacheV111.vehicles.PoolHitInfoMessage;
import diskCacheV111.vehicles.PoolMgrReplicateFileMsg;
import diskCacheV111.vehicles.PoolMgrSelectReadPoolMsg;
import diskCacheV111.vehicles.PoolMgrSelectReadPoolMsg.Context;
import diskCacheV111.vehicles.ProtocolInfo;
import diskCacheV111.vehicles.RestoreHandlerInfo;
import diskCacheV111.vehicles.StorageInfo;
import diskCacheV111.vehicles.WarningPnfsFileInfoMessage;
import dmg.cells.nucleus.CellAddressCore;
import dmg.cells.nucleus.CellEndpoint;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellPath;
import dmg.util.CommandException;
import dmg.util.CommandInterpreter;
import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.IntFunction;
import java.util.stream.Collectors;
import javax.security.auth.Subject;
import org.dcache.auth.Subjects;
import org.dcache.cells.CellStub;
import org.dcache.mock.CacheExceptionBuilder;
import org.dcache.mock.Deliverable;
import org.dcache.mock.DeliveryRepeater;
import org.dcache.mock.EndpointMessageReceiver;
import org.dcache.mock.EnvelopeAndMessageDeliverable;
import org.dcache.mock.FixedTimes;
import org.dcache.mock.PartitionManagerBuilder;
import org.dcache.mock.PoolMonitorBuilder;
import org.dcache.mock.PoolSelectionUnitBuilder;
import org.dcache.mock.PoolStatusChangedBuilder;
import org.dcache.mock.ProtocolInfoBuilder;
import org.dcache.mock.ResponseMessageDeliverable;
import org.dcache.poolmanager.CostException;
import org.dcache.poolmanager.PartitionManager;
import org.dcache.poolmanager.PoolManagerGetRestoreHandlerInfo;
import org.dcache.poolmanager.SelectedPool;
import org.dcache.util.Args;
import org.dcache.util.ExecutorUtils;
import org.dcache.util.FileAttributesBuilder;
import org.dcache.vehicles.FileAttributes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.BDDMockito;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Objects.requireNonNull;
import static org.dcache.auth.Subjects.NOBODY;
import static org.dcache.auth.Subjects.ROOT;
import static org.dcache.mock.PartitionBuilder.aPartition;
import static org.dcache.mock.PartitionManagerBuilder.aPartitionManager;
import static org.dcache.mock.PoolMonitorBuilder.aPoolMonitor;
import static org.dcache.mock.PoolPairBuilder.aPoolPair;
import static org.dcache.mock.PoolSelectionUnitBuilder.aPoolSelectionUnit;
import static org.dcache.mock.PoolSelectorBuilder.aPoolSelectorThat;
import static org.dcache.mock.ProtocolInfoBuilder.aProtocolInfo;
import static org.dcache.mock.SelectedPoolBuilder.aPool;
import static org.dcache.util.ByteUnit.KiB;
import static org.dcache.util.FileAttributesBuilder.attributes;
import static org.dcache.util.StorageInfoBuilder.aStorageInfo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.BDDMockito.then;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class RequestContainerV5Test
{
    private static final Logger LOGGER = LoggerFactory.getLogger(RequestContainerV5Test.class);
    private static final String POOL_GROUP = "thePoolGroup";

    @Mock
    CellStub billing;

    @Mock
    CellStub pool;

    @Mock
    PnfsHandler pnfs;

    @Mock
    CellEndpoint endpoint;

    RequestContainerV5 container;
    PoolSelectionUnit psu;
    PoolMonitorV5 poolMonitor;
    PartitionManager partitionManager;
    ExecutorService executor;
    PoolManagerGetRestoreHandlerInfo infoResponse;
    String commandResponse;

    @Before
    public void setup()
    {
        container = null;
        psu = null;
        poolMonitor = null;
        partitionManager = null;
        infoResponse = null;
        commandResponse = null;
        executor = Executors.newSingleThreadExecutor();
    }

    @After
    public void tearDown() throws InterruptedException
    {
        executor.shutdownNow();
        if (!executor.awaitTermination(100, TimeUnit.MILLISECONDS)) {
            LOGGER.warn("Shutdown Executor still busy after 100 ms...");
        }
    }

    @Test
    public void shouldReplyWithPnfsIdFailure() throws Exception
    {
        given(aPartitionManager().withDefault(aPartition()));
        given(aPoolSelectionUnit().withNetUnit("all-net", "192.168.1.1")
                .withProtocolUnit("HTTP", "http/1"));
        given(aPoolMonitor().thatReturns(aPoolSelectorThat()
                .onReadSelects("pool1@dCacheDomain")));
        given(aContainer("PoolManager@dCacheDomain")
                .thatDoesNotSendHitMessages()
                .withConfig("rc select 80D1B8B90CED30430608C58002811B3285FC"));

        whenReceiving(aReadRequest()
                .forFile("80D1B8B90CED30430608C58002811B3285FC")
                .withBillingPath("/public/test")
                .withTransferPath("/uploads/50/test")
                .withFileAttributes(attributes().size(10, KiB).storageInfo(aStorageInfo()))
                .withProtocolInfo(aProtocolInfo().withProtocol("http")
                        .withMajorVersion(1).withIPAddress("192.168.1.1")));

        var reply = replySentWith(endpoint);
        then(reply).should().setFailed(1, "Failed-1");
        then(reply).should().setContext(1, null);
        then(billing).shouldHaveNoInteractions();
    }

    @Test
    public void shouldReplyWithPnfsIdAndErrorCodeFailure() throws Exception
    {
        given(aPartitionManager().withDefault(aPartition()));
        given(aPoolSelectionUnit().withNetUnit("all-net", "192.168.1.1")
                .withProtocolUnit("HTTP", "http/1"));
        given(aPoolMonitor().thatReturns(aPoolSelectorThat()
                .onReadSelects("pool1@dCacheDomain")));
        given(aContainer("PoolManager@dCacheDomain")
                .thatDoesNotSendHitMessages()
                .withConfig("rc select 80D1B8B90CED30430608C58002811B3285FC 99"));

        whenReceiving(aReadRequest()
                .forFile("80D1B8B90CED30430608C58002811B3285FC")
                .withBillingPath("/public/test")
                .withTransferPath("/uploads/50/test")
                .withFileAttributes(attributes().size(10, KiB).storageInfo(aStorageInfo()))
                .withProtocolInfo(aProtocolInfo().withProtocol("http")
                        .withMajorVersion(1).withIPAddress("192.168.1.1")));

        var reply = replySentWith(endpoint);
        then(reply).should().setFailed(99, "Failed-99");
        then(reply).should().setContext(1, null);
        then(billing).shouldHaveNoInteractions();
    }

    @Test
    public void shouldReplyWithPnfsIdAndErrorCodeAndErrorMessageFailure() throws Exception
    {
        given(aPartitionManager().withDefault(aPartition()));
        given(aPoolSelectionUnit().withNetUnit("all-net", "192.168.1.1")
                .withProtocolUnit("HTTP", "http/1"));
        given(aPoolMonitor().thatReturns(aPoolSelectorThat()
                .onReadSelects("pool1@dCacheDomain")));
        given(aContainer("PoolManager@dCacheDomain")
                .thatDoesNotSendHitMessages()
                .withConfig("rc select 80D1B8B90CED30430608C58002811B3285FC 99 \"Operator intervention\""));

        whenReceiving(aReadRequest()
                .forFile("80D1B8B90CED30430608C58002811B3285FC")
                .withBillingPath("/public/test")
                .withTransferPath("/uploads/50/test")
                .withFileAttributes(attributes().size(10, KiB).storageInfo(aStorageInfo()))
                .withProtocolInfo(aProtocolInfo().withProtocol("http")
                        .withMajorVersion(1).withIPAddress("192.168.1.1")));

        var reply = replySentWith(endpoint);
        then(reply).should().setFailed(99, "Operator intervention");
        then(reply).should().setContext(1, null);
        then(billing).shouldHaveNoInteractions();
    }

    @Test
    public void shouldSuspendRequestWhenSuspendAll() throws Exception
    {
        /* NB. This method tests two things: suspending incoming requests and
         * the retry admin command.  Ideally, these would be two separate tests
         * but we can't know if a request is suspended.
         */
        var storageInfo = aStorageInfo().build();
        given(aPartitionManager().withDefault(aPartition()));
        given(aPoolSelectionUnit().withNetUnit("all-net", "192.168.1.1")
                .withProtocolUnit("HTTP", "http/1"));
        given(aPoolMonitor().thatReturns(aPoolSelectorThat()
                .onReadSelects("pool1@dCacheDomain")));
        given(aContainer("PoolManager@dCacheDomain")
                .thatDoesNotSendHitMessages()
                .withConfig("rc suspend on -all"));

        whenReceiving(aReadRequest()
                .forFile("80D1B8B90CED30430608C58002811B3285FC")
                .withBillingPath("/public/test")
                .withTransferPath("/uploads/50/test")
                .withFileAttributes(attributes().size(10, KiB).storageInfo(storageInfo))
                .withProtocolInfo(aProtocolInfo().withProtocol("http")
                        .withMajorVersion(1).withIPAddress("192.168.1.1")));

        then(endpoint).shouldHaveNoInteractions();

        var warning = notificationSentWith(billing, WarningPnfsFileInfoMessage.class);
        assertThat(warning.getResultCode(), equalTo(0));
        assertThat(warning.getCellAddress(), equalTo(new CellAddressCore("PoolManager@dCacheDomain")));
        assertThat(warning.getBillingPath(), equalTo("/public/test"));
        assertThat(warning.getTransferPath(), equalTo("/uploads/50/test"));
        assertThat(warning.getFileSize(), equalTo(KiB.toBytes(10L)));
        assertThat(warning.getStorageInfo(), is(storageInfo));

        whenExecutedAdminCommand("rc retry * -force-all");

        var reply = replySentWith(endpoint);
        then(reply).should().setFailed(eq(10021), any());
        then(reply).should().setContext(0, null);
        then(billing).shouldHaveNoMoreInteractions();
    }

    @Test
    public void shouldSendPoolHitInfoForSimpleReadRequest() throws Exception
    {
        var storageInfo = aStorageInfo().build();
        var protocolInfo = aProtocolInfo().withProtocol("http").withMajorVersion(1)
                        .withIPAddress("192.168.1.1").build();
        given(aPartitionManager().withDefault(aPartition()));
        given(aPoolSelectionUnit().withNetUnit("all-net", "192.168.1.1")
                .withProtocolUnit("HTTP", "http/1"));
        given(aPoolMonitor().thatReturns(aPoolSelectorThat()
                .onReadSelects("pool1@dCacheDomain")));
        given(aContainer("PoolManager@dCacheDomain").thatSendsHitMessages());

        whenReceiving(aReadRequest()
                .forFile("80D1B8B90CED30430608C58002811B3285FC")
                .withBillingPath("/public/test")
                .withTransferPath("/uploads/50/test")
                .withFileAttributes(attributes().size(10, KiB).storageInfo(storageInfo))
                .withProtocolInfo(protocolInfo));

        var info = notificationSentWith(billing, PoolHitInfoMessage.class);
        assertThat(info.getCellAddress(), equalTo(new CellAddressCore("pool1@dCacheDomain")));
        assertThat(info.getBillingPath(), equalTo("/public/test"));
        assertThat(info.getTransferPath(), equalTo("/uploads/50/test"));
        assertThat(info.getFileSize(), equalTo(KiB.toBytes(10L)));
        assertThat(info.getStorageInfo(), is(storageInfo));
        assertThat(info.getProtocolInfo(), is(protocolInfo));
    }

    @Test
    public void shouldNotSendPoolHitInfoForSimpleReadRequest() throws Exception
    {
        given(aPartitionManager().withDefault(aPartition()));
        given(aPoolSelectionUnit().withNetUnit("all-net", "192.168.1.1")
                .withProtocolUnit("HTTP", "http/1"));
        given(aPoolMonitor().thatReturns(aPoolSelectorThat()
                .onReadSelects("pool1@dCacheDomain")));
        given(aContainer("PoolManager@dCacheDomain").thatDoesNotSendHitMessages());

        whenReceiving(aReadRequest()
                .forFile("80D1B8B90CED30430608C58002811B3285FC")
                .withPath("/public/test")
                .withFileAttributes(attributes().size(10, KiB).storageInfo(aStorageInfo()))
                .withProtocolInfo(aProtocolInfo().withProtocol("http").withMajorVersion(1)
                        .withIPAddress("192.168.1.1")));

        then(billing).shouldHaveNoInteractions();
    }

    @Test
    public void shouldReplySuccessForSimpleReadRequest() throws Exception
    {
        given(aPartitionManager().withDefault(aPartition()));
        given(aPoolSelectionUnit().withNetUnit("all-net", "192.168.1.1")
                .withProtocolUnit("HTTP", "http/1"));
        given(aPoolMonitor().thatReturns(aPoolSelectorThat()
                .onReadSelects("pool1@dCacheDomain")));
        given(aContainer("PoolManager@dCacheDomain").thatDoesNotSendHitMessages());

        whenReceiving(aReadRequest()
                .forFile("80D1B8B90CED30430608C58002811B3285FC")
                .withBillingPath("/public/test")
                .withTransferPath("/uploads/50/test")
                .withFileAttributes(attributes().size(10, KiB).storageInfo(aStorageInfo()))
                .withProtocolInfo(aProtocolInfo().withProtocol("http")
                        .withMajorVersion(1).withIPAddress("192.168.1.1")));

        var reply = replySentWith(endpoint);
        then(reply).should().setSucceeded();
        then(reply).should().setContext(1, null);
        var selectedPool = poolSetInMessage(reply);
        assertThat(selectedPool.getName(), equalTo("pool1"));
        assertThat(selectedPool.getAddress(), equalTo(new CellAddressCore("pool1@dCacheDomain")));
    }

    @Test
    public void shouldSuspendNonCachedFileWithHsmDisabled() throws Exception
    {
        var storageInfo = aStorageInfo().withLocation("osm://RZ1/bfid1").build();
        given(aPartitionManager().withDefault(aPartition().withStageAllowed(false)));
        given(aPoolSelectionUnit().withNetUnit("all-net", "192.168.1.1")
                .withProtocolUnit("HTTP", "http/1"));
        given(aPoolMonitor().thatReturns(aPoolSelectorThat()
                .onReadThrows(aFileNotInCacheException())));
        given(aContainer("PoolManager@dCacheDomain")
                .thatDoesNotSendHitMessages()
                .withConfig("rc onerror suspend"));

        whenReceiving(aReadRequest()
                .forFile("80D1B8B90CED30430608C58002811B3285FC")
                .withBillingPath("/public/test")
                .withTransferPath("/uploads/50/test")
                .withFileAttributes(attributes().size(10, KiB).storageInfo(storageInfo))
                .withProtocolInfo(aProtocolInfo().withProtocol("http")
                        .withMajorVersion(1).withIPAddress("192.168.1.1")));

        then(endpoint).shouldHaveNoInteractions();

        var warning = notificationSentWith(billing, WarningPnfsFileInfoMessage.class);
        assertThat(warning.getResultCode(), equalTo(0));
        assertThat(warning.getCellAddress(), equalTo(new CellAddressCore("PoolManager@dCacheDomain")));
        assertThat(warning.getBillingPath(), equalTo("/public/test"));
        assertThat(warning.getTransferPath(), equalTo("/uploads/50/test"));
        assertThat(warning.getFileSize(), equalTo(KiB.toBytes(10L)));
        assertThat(warning.getStorageInfo(), is(storageInfo));

        whenExecutedAdminCommand("rc retry * -force-all");

        var reply = replySentWith(endpoint);
        then(reply).should().setFailed(eq(10021), any());
        then(reply).should().setContext(0, null);
        then(billing).shouldHaveNoMoreInteractions();
    }

    @Test
    public void shouldSendHitInfoForNotInCacheException() throws Exception
    {
        var protocolInfo = aProtocolInfo().withProtocol("http").withMajorVersion(1)
                .withIPAddress("192.168.1.1").build();
        StorageInfo storageInfo = aStorageInfo().build();
        given(aPartitionManager().withDefault(aPartition().withStageAllowed(false)));
        given(aPoolSelectionUnit().withNetUnit("all-net", "192.168.1.1")
                .withProtocolUnit("HTTP", "http/1"));
        given(aPoolMonitor().thatReturns(aPoolSelectorThat()
                .onReadThrows(aFileNotInCacheException())));
        given(aContainer("PoolManager@dCacheDomain").thatSendsHitMessages());

        whenReceiving(aReadRequest()
                .forFile("80D1B8B90CED30430608C58002811B3285FC")
                .withBillingPath("/public/test")
                .withTransferPath("/uploads/50/test")
                .withFileAttributes(attributes().size(10, KiB).storageInfo(storageInfo))
                .withProtocolInfo(protocolInfo));

        var info = notificationSentWith(billing, PoolHitInfoMessage.class);
        assertThat(info.getCellAddress(), equalTo(null));
        assertThat(info.getBillingPath(), equalTo("/public/test"));
        assertThat(info.getTransferPath(), equalTo("/uploads/50/test"));
        assertThat(info.getFileSize(), equalTo(KiB.toBytes(10L)));
        assertThat(info.getFileCached(), equalTo(false));
        assertThat(info.getStorageInfo(), is(storageInfo));
        assertThat(info.getProtocolInfo(), is(protocolInfo));
    }

    @Test
    public void shouldSuspendLostFileWithHsmEnabled() throws Exception
    {
        var storageInfo = aStorageInfo().build();
        given(aPartitionManager().withDefault(aPartition().withStageAllowed(true)));
        given(aPoolSelectionUnit().withNetUnit("all-net", "192.168.1.1")
                .withProtocolUnit("HTTP", "http/1"));
        given(aPoolMonitor().thatReturns(aPoolSelectorThat()
                .onReadThrows(aFileNotInCacheException())));
        given(aContainer("PoolManager@dCacheDomain")
                .thatDoesNotSendHitMessages()
                .withConfig("rc onerror suspend"));

        whenReceiving(aReadRequest()
                .forFile("80D1B8B90CED30430608C58002811B3285FC")
                .withBillingPath("/public/test")
                .withTransferPath("/uploads/50/test")
                .withFileAttributes(attributes().size(10, KiB).storageInfo(storageInfo))
                .withProtocolInfo(aProtocolInfo().withProtocol("http")
                        .withMajorVersion(1).withIPAddress("192.168.1.1")));

        then(endpoint).shouldHaveNoInteractions();

        var warning = notificationSentWith(billing, WarningPnfsFileInfoMessage.class);
        assertThat(warning.getResultCode(), equalTo(0));
        assertThat(warning.getCellAddress(), equalTo(new CellAddressCore("PoolManager@dCacheDomain")));
        assertThat(warning.getBillingPath(), equalTo("/public/test"));
        assertThat(warning.getTransferPath(), equalTo("/uploads/50/test"));
        assertThat(warning.getFileSize(), equalTo(KiB.toBytes(10L)));
        assertThat(warning.getStorageInfo(), is(storageInfo));

        whenExecutedAdminCommand("rc retry * -force-all");

        var reply = replySentWith(endpoint);
        then(reply).should().setFailed(eq(10021), any());
        then(reply).should().setContext(0, null);
        then(billing).shouldHaveNoMoreInteractions();
    }

    @Test
    public void shouldFailReadRequestIfCostExceededWithTryAlternativesAndNoFallback() throws Exception
    {
        given(aPartitionManager().withDefault(aPartition()));
        given(aPoolSelectionUnit().withNetUnit("all-net", "192.168.1.1")
                .withProtocolUnit("HTTP", "http/1"));
        given(aPoolMonitor().thatReturns(aPoolSelectorThat()
                .onReadThrows(aCostException()
                        .withPool("hot-pool@dCacheDomain")
                        .withTryAlternatives(true))
                .returnsCurrentPartition(aPartition().withStageAllowed(true).withStageOnCost(false).withP2pOnCost(false))));
        given(aContainer("PoolManager@dCacheDomain").thatDoesNotSendHitMessages());

        whenReceiving(aReadRequest()
                .forFile("80D1B8B90CED30430608C58002811B3285FC")
                .withBillingPath("/public/test")
                .withTransferPath("/uploads/50/test")
                .withFileAttributes(attributes().size(10, KiB).storageInfo(aStorageInfo()))
                .withProtocolInfo(aProtocolInfo().withProtocol("http")
                        .withMajorVersion(1).withIPAddress("192.168.1.1")));

        var reply = replySentWith(endpoint);
        then(reply).should().setFailed(eq(127), any());
        then(reply).should().setContext(1, null);
        then(billing).shouldHaveNoInteractions();
    }

    @Test
    public void shouldFailReadRequestIfCostExceededWithTryAlternativesAndNoP2pOnCostAndNoHsm() throws Exception
    {
        given(aPartitionManager().withDefault(aPartition()));
        given(aPoolSelectionUnit().withNetUnit("all-net", "192.168.1.1")
                .withProtocolUnit("HTTP", "http/1"));
        given(aPoolMonitor().thatReturns(aPoolSelectorThat()
                .onReadThrows(aCostException()
                        .withPool("hot-pool@dCacheDomain")
                        .withTryAlternatives(true))
                .returnsCurrentPartition(aPartition().withStageAllowed(false).withP2pOnCost(false))));
        given(aContainer("PoolManager@dCacheDomain").thatDoesNotSendHitMessages());

        whenReceiving(aReadRequest()
                .forFile("80D1B8B90CED30430608C58002811B3285FC")
                .withBillingPath("/public/test")
                .withTransferPath("/uploads/50/test")
                .withFileAttributes(attributes().size(10, KiB).storageInfo(aStorageInfo()))
                .withProtocolInfo(aProtocolInfo().withProtocol("http")
                        .withMajorVersion(1).withIPAddress("192.168.1.1")));

        var reply = replySentWith(endpoint);
        then(reply).should().setFailed(eq(127), any());
        then(reply).should().setContext(1, null);
        then(billing).shouldHaveNoInteractions();
    }

    @Test
    public void shouldSendPoolAPool2PoolRequestForReplicateRequest() throws Exception
    {
        var fileAttributes = attributes().size(10, KiB).storageInfo(aStorageInfo()).build();
        given(aPartitionManager().withDefault(aPartition().withP2pAllowed(false)));
        given(aPoolSelectionUnit().withNetUnit("all-net", "192.168.1.1")
                .withProtocolUnit("HTTP", "http/1"));
        given(aPoolMonitor().thatReturns(aPoolSelectorThat()
                .onPool2PoolSelects(aPoolPair()
                        .withSource("source-pool@dCacheDomain")
                        .withDestination("destination-pool@dCacheDomain"))));
        given(aContainer("PoolManager@dCacheDomain").thatDoesNotSendHitMessages());

        whenReceiving(aReplicateRequest()
                .forFile("80D1B8B90CED30430608C58002811B3285FC")
                .withPath("/public/test")
                .withFileAttributes(fileAttributes)
                .withProtocolInfo(aProtocolInfo().withProtocol("http")
                        .withMajorVersion(1).withIPAddress("192.168.1.1")));

        var messageToPool = envelopeSentWith(endpoint);
        assertThat(messageToPool.getDestinationPath(), equalTo(new CellPath("destination-pool@dCacheDomain")));

        var message = (Pool2PoolTransferMsg) messageToPool.getMessageObject();
        assertThat(message.getPnfsId(), equalTo(new PnfsId("80D1B8B90CED30430608C58002811B3285FC")));
        assertThat(message.getDestinationFileStatus(), equalTo(Pool2PoolTransferMsg.UNDETERMINED));
        assertThat(message.getDestinationPoolName(), equalTo("destination-pool"));
        assertThat(message.getSourcePoolName(), equalTo("source-pool"));
        assertThat(message.getFileAttributes(), equalTo(fileAttributes));
        then(billing).shouldHaveNoInteractions();
    }

    @Test
    public void shouldDuplicateFileForReplicateRequest() throws Exception
    {
        given(aPartitionManager().withDefault(aPartition().withP2pAllowed(false)));
        given(aPoolSelectionUnit().withNetUnit("all-net", "192.168.1.1")
                .withProtocolUnit("HTTP", "http/1"));
        given(aPoolMonitor().thatReturns(aPoolSelectorThat()
                .onPool2PoolSelects(aPoolPair()
                        .withSource("source-pool@dCacheDomain")
                        .withDestination("destination-pool@dCacheDomain"))));
        given(aCell("destination-pool@dCacheDomain").communicatingVia(endpoint)
            .thatOnReceiving(Pool2PoolTransferMsg.class).repliesWithSuccess());
        given(aContainer("PoolManager@dCacheDomain").thatDoesNotSendHitMessages());

        whenReceiving(aReplicateRequest()
                .forFile("80D1B8B90CED30430608C58002811B3285FC")
                .withPath("/public/test")
                .withFileAttributes(attributes().size(10, KiB).storageInfo(aStorageInfo()))
                .withProtocolInfo(aProtocolInfo().withProtocol("http")
                        .withMajorVersion(1).withIPAddress("192.168.1.1")));

        var reply = replySentWith(endpoint);
        then(reply).should().setSucceeded();
        then(reply).should().setContext(1, null);
        var selectedPool = poolSetInMessage(reply);
        assertThat(selectedPool.getName(), equalTo("destination-pool"));
        assertThat(selectedPool.getAddress(), equalTo(new CellAddressCore("destination-pool@dCacheDomain")));
        then(billing).shouldHaveNoInteractions();
    }

    @Test
    public void shouldDuplicateFileWithHitInfoForReplicateRequest() throws Exception
    {
        var storageInfo = aStorageInfo().build();
        var protocolInfo = aProtocolInfo().withProtocol("http").withMajorVersion(1)
                .withIPAddress("192.168.1.1").build();
        given(aPartitionManager().withDefault(aPartition().withP2pAllowed(false)));
        given(aPoolSelectionUnit().withNetUnit("all-net", "192.168.1.1")
                .withProtocolUnit("HTTP", "http/1"));
        given(aPoolMonitor().thatReturns(aPoolSelectorThat()
                .onPool2PoolSelects(aPoolPair()
                        .withSource("source-pool@dCacheDomain")
                        .withDestination("destination-pool@dCacheDomain"))));
        given(aCell("destination-pool@dCacheDomain").communicatingVia(endpoint)
                .thatOnReceiving(Pool2PoolTransferMsg.class).repliesWithSuccess());
        given(aContainer("PoolManager@dCacheDomain").thatSendsHitMessages());

        whenReceiving(aReplicateRequest()
                .forFile("80D1B8B90CED30430608C58002811B3285FC")
                .withPath("/public/test")
                .withFileAttributes(attributes().size(10, KiB).storageInfo(storageInfo))
                .withProtocolInfo(protocolInfo));

        var info = notificationSentWith(billing, PoolHitInfoMessage.class);
        assertThat(info.getCellAddress(), equalTo(new CellAddressCore("source-pool@dCacheDomain")));
        assertThat(info.getBillingPath(), equalTo("/public/test"));
        assertThat(info.getTransferPath(), equalTo("/public/test"));
        assertThat(info.getFileSize(), equalTo(KiB.toBytes(10L)));
        assertThat(info.getStorageInfo(), is(storageInfo));
        assertThat(info.getProtocolInfo(), is(protocolInfo));

        var reply = replySentWith(endpoint);
        then(reply).should().setSucceeded();
        then(reply).should().setContext(1, null);
        var selectedPool = poolSetInMessage(reply);
        assertThat(selectedPool.getName(), equalTo("destination-pool"));
        assertThat(selectedPool.getAddress(), equalTo(new CellAddressCore("destination-pool@dCacheDomain")));
    }

    @Test
    public void shouldPool2PoolIfCostExceededWithPool2PoolOnCost() throws Exception
    {
        given(aPartitionManager().withDefault(aPartition()));
        given(aPoolSelectionUnit().withNetUnit("all-net", "192.168.1.1")
                .withProtocolUnit("HTTP", "http/1"));
        given(aPoolMonitor().thatReturns(aPoolSelectorThat()
                .returnsCurrentPartition(aPartition().withP2pOnCost(true))
                .onReadThrows(aCostException()
                        .withPool("hot-pool@dCacheDomain")
                        .withTryAlternatives(true))
                .onPool2PoolSelects(aPoolPair()
                        .withSource("source-pool@dCacheDomain")
                        .withDestination("destination-pool@dCacheDomain"))));
        given(aCell("destination-pool@dCacheDomain").communicatingVia(endpoint)
                .thatOnReceiving(Pool2PoolTransferMsg.class).repliesWithSuccess());
        given(aContainer("PoolManager@dCacheDomain").thatDoesNotSendHitMessages());

        whenReceiving(aReadRequest()
                .forFile("80D1B8B90CED30430608C58002811B3285FC")
                .withBillingPath("/public/test")
                .withTransferPath("/uploads/50/test")
                .withFileAttributes(attributes().size(10, KiB).storageInfo(aStorageInfo()))
                .withProtocolInfo(aProtocolInfo().withProtocol("http")
                        .withMajorVersion(1).withIPAddress("192.168.1.1")));

        var reply = replySentWith(endpoint);
        then(reply).should().setSucceeded();
        then(reply).should().setContext(1, null);
        var selectedPool = poolSetInMessage(reply);
        assertThat(selectedPool.getName(), equalTo("destination-pool"));
        assertThat(selectedPool.getAddress(), equalTo(new CellAddressCore("destination-pool@dCacheDomain")));
        then(billing).shouldHaveNoInteractions();
    }

    @Test
    public void shouldPool2PoolIfPermissionDeniedAndP2pAllowed() throws Exception
    {
        given(aPartitionManager().withDefault(aPartition().withP2pAllowed(true)));
        given(aPoolSelectionUnit().withNetUnit("all-net", "192.168.1.1")
                .withProtocolUnit("HTTP", "http/1"));
        given(aPoolMonitor().thatReturns(aPoolSelectorThat()
                .onReadThrows(aPermissionDeniedCacheException())
                .onPool2PoolSelects(aPoolPair()
                        .withSource("source-pool@dCacheDomain")
                        .withDestination("destination-pool@dCacheDomain"))));
        given(aCell("destination-pool@dCacheDomain").communicatingVia(endpoint)
                .thatOnReceiving(Pool2PoolTransferMsg.class).repliesWithSuccess());
        given(aContainer("PoolManager@dCacheDomain").thatDoesNotSendHitMessages());

        whenReceiving(aReadRequest()
                .forFile("80D1B8B90CED30430608C58002811B3285FC")
                .withBillingPath("/public/test")
                .withTransferPath("/uploads/50/test")
                .withFileAttributes(attributes().size(10, KiB).storageInfo(aStorageInfo()))
                .withProtocolInfo(aProtocolInfo().withProtocol("http")
                        .withMajorVersion(1).withIPAddress("192.168.1.1")));

        var reply = replySentWith(endpoint);
        then(reply).should().setSucceeded();
        then(reply).should().setContext(1, null);
        var selectedPool = poolSetInMessage(reply);
        assertThat(selectedPool.getName(), equalTo("destination-pool"));
        assertThat(selectedPool.getAddress(), equalTo(new CellAddressCore("destination-pool@dCacheDomain")));
        then(billing).shouldHaveNoInteractions();
    }

    @Test
    public void shouldPool2PoolIfPermissionDeniedAndP2pDeniedAndHsmDenied() throws Exception
    {
        given(aPartitionManager().withDefault(aPartition().withP2pAllowed(false).withStageAllowed(false)));
        given(aPoolSelectionUnit().withNetUnit("all-net", "192.168.1.1")
                .withProtocolUnit("HTTP", "http/1"));
        given(aPoolMonitor().thatReturns(aPoolSelectorThat()
                .onReadThrows(aPermissionDeniedCacheException())
                .onPool2PoolSelects(aPoolPair()
                        .withSource("source-pool@dCacheDomain")
                        .withDestination("destination-pool@dCacheDomain"))));
        given(aCell("destination-pool@dCacheDomain").communicatingVia(endpoint)
                .thatOnReceiving(Pool2PoolTransferMsg.class).repliesWithSuccess());
        given(aContainer("PoolManager@dCacheDomain").thatDoesNotSendHitMessages());

        whenReceiving(aReadRequest()
                .forFile("80D1B8B90CED30430608C58002811B3285FC")
                .withBillingPath("/public/test")
                .withTransferPath("/uploads/50/test")
                .withFileAttributes(attributes().size(10, KiB).storageInfo(aStorageInfo()))
                .withProtocolInfo(aProtocolInfo().withProtocol("http")
                        .withMajorVersion(1).withIPAddress("192.168.1.1")));

        var reply = replySentWith(endpoint);
        then(reply).should().setSucceeded();
        then(reply).should().setContext(1, null);
        var selectedPool = poolSetInMessage(reply);
        assertThat(selectedPool.getName(), equalTo("destination-pool"));
        assertThat(selectedPool.getAddress(), equalTo(new CellAddressCore("destination-pool@dCacheDomain")));
        then(billing).shouldHaveNoInteractions();
    }

    @Test
    public void shouldStageIfCostExceededWithNoHotPool() throws Exception
    {
        var stagePool = aPool("stage-pool@dCacheDomain");
        given(aPartitionManager().withDefault(aPartition()));
        given(aPoolSelectionUnit().withNetUnit("all-net", "192.168.1.1")
                .withProtocolUnit("HTTP", "http/1"));
        given(aPoolMonitor().thatReturns(aPoolSelectorThat()
                .onReadThrows(aCostException())
                .onStageSelects(stagePool)));
        given(aCell("stage-pool@dCacheDomain").communicatingVia(endpoint)
                .thatOnReceiving(PoolFetchFileMessage.class).repliesWithSuccess());
        given(aContainer("PoolManager@dCacheDomain").thatDoesNotSendHitMessages());

        whenReceiving(aReadRequest()
                .by(ROOT)
                .forFile("80D1B8B90CED30430608C58002811B3285FC")
                .withBillingPath("/public/test")
                .withTransferPath("/uploads/50/test")
                .withFileAttributes(attributes().size(10, KiB).storageInfo(aStorageInfo()))
                .withProtocolInfo(aProtocolInfo().withProtocol("http")
                        .withMajorVersion(1).withIPAddress("192.168.1.1")));

        var reply = replySentWith(endpoint);
        then(reply).should().setSucceeded();
        then(reply).should().setContext(1, stagePool);
        var selectedPool = poolSetInMessage(reply);
        assertThat(selectedPool.getName(), equalTo("stage-pool"));
        assertThat(selectedPool.getAddress(), equalTo(new CellAddressCore("stage-pool@dCacheDomain")));
        then(billing).shouldHaveNoInteractions();
    }

    @Test
    public void shouldStageIfReadSelectionThrowsCacheException() throws Exception
    {
        var stagePool = aPool("stage-pool@dCacheDomain");
        given(aPartitionManager().withDefault(aPartition()));
        given(aPoolSelectionUnit().withNetUnit("all-net", "192.168.1.1")
                .withProtocolUnit("HTTP", "http/1"));
        given(aPoolMonitor().thatReturns(aPoolSelectorThat()
                .onReadThrows(aCacheException())
                .onStageSelects(stagePool)));
        given(aCell("stage-pool@dCacheDomain").communicatingVia(endpoint)
                .thatOnReceiving(PoolFetchFileMessage.class).repliesWithSuccess());
        given(aContainer("PoolManager@dCacheDomain").thatDoesNotSendHitMessages());

        whenReceiving(aReadRequest()
                .by(ROOT)
                .forFile("80D1B8B90CED30430608C58002811B3285FC")
                .withBillingPath("/public/test")
                .withTransferPath("/uploads/50/test")
                .withFileAttributes(attributes().size(10, KiB).storageInfo(aStorageInfo()))
                .withProtocolInfo(aProtocolInfo().withProtocol("http")
                        .withMajorVersion(1).withIPAddress("192.168.1.1")));

        var reply = replySentWith(endpoint);
        then(reply).should().setSucceeded();
        then(reply).should().setContext(1, stagePool);
        var selectedPool = poolSetInMessage(reply);
        assertThat(selectedPool.getName(), equalTo("stage-pool"));
        assertThat(selectedPool.getAddress(), equalTo(new CellAddressCore("stage-pool@dCacheDomain")));
        then(billing).shouldHaveNoInteractions();
    }

    @Test
    public void shouldStageIfReadSelectionThrowsIllegalArgumentException() throws Exception
    {
        var stagePool = aPool("stage-pool@dCacheDomain");
        given(aPartitionManager().withDefault(aPartition()));
        given(aPoolSelectionUnit().withNetUnit("all-net", "192.168.1.1")
                .withProtocolUnit("HTTP", "http/1"));
        given(aPoolMonitor().thatReturns(aPoolSelectorThat()
                .onReadThrows(anIllegalArgumentException())
                .onStageSelects(stagePool)));
        given(aCell("stage-pool@dCacheDomain").communicatingVia(endpoint)
                .thatOnReceiving(PoolFetchFileMessage.class).repliesWithSuccess());
        given(aContainer("PoolManager@dCacheDomain").thatDoesNotSendHitMessages());

        whenReceiving(aReadRequest()
                .by(ROOT)
                .forFile("80D1B8B90CED30430608C58002811B3285FC")
                .withBillingPath("/public/test")
                .withTransferPath("/uploads/50/test")
                .withFileAttributes(attributes().size(10, KiB).storageInfo(aStorageInfo()))
                .withProtocolInfo(aProtocolInfo().withProtocol("http")
                        .withMajorVersion(1).withIPAddress("192.168.1.1")));

        var reply = replySentWith(endpoint);
        then(reply).should().setSucceeded();
        then(reply).should().setContext(1, stagePool);
        var selectedPool = poolSetInMessage(reply);
        assertThat(selectedPool.getName(), equalTo("stage-pool"));
        assertThat(selectedPool.getAddress(), equalTo(new CellAddressCore("stage-pool@dCacheDomain")));
        then(billing).shouldHaveNoInteractions();
    }

    @Test
    public void shouldFailIfReadSelectionThrowsRuntimeException() throws Exception
    {
        given(aPartitionManager().withDefault(aPartition()));
        given(aPoolSelectionUnit().withNetUnit("all-net", "192.168.1.1")
                .withProtocolUnit("HTTP", "http/1"));
        given(aPoolMonitor().thatReturns(aPoolSelectorThat()
                .onReadThrows(aRuntimeException())));
        given(aContainer("PoolManager@dCacheDomain").thatDoesNotSendHitMessages());

        whenReceiving(aReadRequest()
                .by(ROOT)
                .forFile("80D1B8B90CED30430608C58002811B3285FC")
                .withBillingPath("/public/test")
                .withTransferPath("/uploads/50/test")
                .withFileAttributes(attributes().size(10, KiB).storageInfo(aStorageInfo()))
                .withProtocolInfo(aProtocolInfo().withProtocol("http")
                        .withMajorVersion(1).withIPAddress("192.168.1.1")));

        var reply = replySentWith(endpoint);
        then(reply).should().setFailed(eq(10011), any());
        then(reply).should().setContext(1, null);
        then(billing).shouldHaveNoInteractions();
    }

    @Test
    public void shouldStageTapeFileIfNoReplicaOnPool() throws Exception
    {
        var stagePool = aPool("stage-pool@dCacheDomain");
        given(aPartitionManager().withDefault(aPartition().withStageAllowed(true)));
        given(aPoolSelectionUnit().withNetUnit("all-net", "192.168.1.1")
                .withProtocolUnit("HTTP", "http/1"));
        given(aPoolMonitor().thatReturns(aPoolSelectorThat()
                .onReadThrows(aFileNotInCacheException())
                .onStageSelects(stagePool)));
        given(aCell("stage-pool@dCacheDomain").communicatingVia(endpoint)
                .thatOnReceiving(PoolFetchFileMessage.class).repliesWithSuccess());
        given(aContainer("PoolManager@dCacheDomain").thatDoesNotSendHitMessages());

        whenReceiving(aReadRequest()
                .by(ROOT)
                .forFile("80D1B8B90CED30430608C58002811B3285FC")
                .withBillingPath("/public/test")
                .withTransferPath("/uploads/50/test")
                .withFileAttributes(attributes().size(10, KiB).storageInfo(aStorageInfo().withLocation("osm://RZ1/bfid1")))
                .withProtocolInfo(aProtocolInfo().withProtocol("http")
                        .withMajorVersion(1).withIPAddress("192.168.1.1")));

        var reply = replySentWith(endpoint);
        then(reply).should().setSucceeded();
        then(reply).should().setContext(1, stagePool);
        var selectedPool = poolSetInMessage(reply);
        assertThat(selectedPool.getName(), equalTo("stage-pool"));
        assertThat(selectedPool.getAddress(), equalTo(new CellAddressCore("stage-pool@dCacheDomain")));
        then(billing).shouldHaveNoInteractions();
    }

    @Test
    public void shouldStageFileIfPermissionDeniedWithP2pDisabledAndHsmEnabled() throws Exception
    {
        var stagePool = aPool("stage-pool@dCacheDomain");
        given(aPartitionManager().withDefault(aPartition().withP2pAllowed(false).withStageAllowed(true)));
        given(aPoolSelectionUnit().withNetUnit("all-net", "192.168.1.1")
                .withProtocolUnit("HTTP", "http/1"));
        given(aPoolMonitor().thatReturns(aPoolSelectorThat()
                .onReadThrows(aPermissionDeniedCacheException())
                .onStageSelects(stagePool)));
        given(aCell("stage-pool@dCacheDomain").communicatingVia(endpoint)
                .thatOnReceiving(PoolFetchFileMessage.class).repliesWithSuccess());
        given(aContainer("PoolManager@dCacheDomain").thatDoesNotSendHitMessages());

        whenReceiving(aReadRequest()
                .by(ROOT)
                .forFile("80D1B8B90CED30430608C58002811B3285FC")
                .withBillingPath("/public/test")
                .withTransferPath("/uploads/50/test")
                .withFileAttributes(attributes().size(10, KiB).storageInfo(aStorageInfo().withLocation("osm://RZ1/bfid1")))
                .withProtocolInfo(aProtocolInfo().withProtocol("http")
                        .withMajorVersion(1).withIPAddress("192.168.1.1")));

        var reply = replySentWith(endpoint);
        then(reply).should().setSucceeded();
        then(reply).should().setContext(1, stagePool);
        var selectedPool = poolSetInMessage(reply);
        assertThat(selectedPool.getName(), equalTo("stage-pool"));
        assertThat(selectedPool.getAddress(), equalTo(new CellAddressCore("stage-pool@dCacheDomain")));
        then(billing).shouldHaveNoInteractions();
    }

    @Test
    public void shouldFailIfCostExceededForDiskFile() throws Exception
    {
        given(aPartitionManager().withDefault(aPartition()));
        given(aPoolSelectionUnit().withNetUnit("all-net", "192.168.1.1")
                .withProtocolUnit("HTTP", "http/1"));
        given(aPoolMonitor().thatReturns(aPoolSelectorThat()
                .onReadThrows(aCostException()
                        .withPool("hot-pool@dCacheDomain")
                        .withTryAlternatives(true))
                .returnsCurrentPartition(aPartition().withStageAllowed(true).withStageOnCost(true).withP2pOnCost(false))));
        given(aCell("stage-pool@dCacheDomain").communicatingVia(endpoint)
                .thatOnReceiving(PoolFetchFileMessage.class).repliesWithSuccess());
        given(aContainer("PoolManager@dCacheDomain").thatDoesNotSendHitMessages());

        whenReceiving(aReadRequest()
                .by(ROOT)
                .forFile("80D1B8B90CED30430608C58002811B3285FC")
                .withBillingPath("/public/test")
                .withTransferPath("/uploads/50/test")
                .withFileAttributes(attributes().size(10, KiB).storageInfo(aStorageInfo()))
                .withProtocolInfo(aProtocolInfo().withProtocol("http")
                        .withMajorVersion(1).withIPAddress("192.168.1.1")));

        var reply = replySentWith(endpoint);
        then(reply).should().setFailed(eq(127), any());
        then(reply).should().setContext(1, null);
        then(billing).shouldHaveNoInteractions();
    }

    @Test
    public void shouldStageIfCostExceededForTapeFile() throws Exception
    {
        var stagePool = aPool("stage-pool@dCacheDomain");
        given(aPartitionManager().withDefault(aPartition()));
        given(aPoolSelectionUnit().withNetUnit("all-net", "192.168.1.1")
                .withProtocolUnit("HTTP", "http/1"));
        given(aPoolMonitor().thatReturns(aPoolSelectorThat()
                .onReadThrows(aCostException()
                        .withPool("hot-pool@dCacheDomain")
                        .withTryAlternatives(true))
                .returnsCurrentPartition(aPartition().withStageAllowed(true).withStageOnCost(true))
                .onStageSelects(stagePool)));
        given(aCell("stage-pool@dCacheDomain").communicatingVia(endpoint)
                .thatOnReceiving(PoolFetchFileMessage.class).repliesWithSuccess());
        given(aContainer("PoolManager@dCacheDomain").thatDoesNotSendHitMessages());

        whenReceiving(aReadRequest()
                .by(ROOT)
                .forFile("80D1B8B90CED30430608C58002811B3285FC")
                .withBillingPath("/public/test")
                .withTransferPath("/uploads/50/test")
                .withFileAttributes(attributes().size(10, KiB)
                        .storageInfo(aStorageInfo().withLocation("osm://RZ1/bfid1")))
                .withProtocolInfo(aProtocolInfo().withProtocol("http")
                        .withMajorVersion(1).withIPAddress("192.168.1.1")));

        var reply = replySentWith(endpoint);
        then(reply).should().setSucceeded();
        then(reply).should().setContext(1, stagePool);
        var selectedPool = poolSetInMessage(reply);
        assertThat(selectedPool.getName(), equalTo("stage-pool"));
        assertThat(selectedPool.getAddress(), equalTo(new CellAddressCore("stage-pool@dCacheDomain")));
        then(billing).shouldHaveNoInteractions();
    }

    @Test
    public void shouldFailForReplicateRequestWhenP2pPermissionDenied() throws Exception
    {
        given(aPartitionManager().withDefault(aPartition()));
        given(aPoolSelectionUnit().withNetUnit("all-net", "192.168.1.1")
                .withProtocolUnit("HTTP", "http/1"));
        given(aPoolMonitor().thatReturns(aPoolSelectorThat()
                .onPool2PoolThrows(aPermissionDeniedCacheException())));
        given(aContainer("PoolManager@dCacheDomain").thatDoesNotSendHitMessages());

        whenReceiving(aReplicateRequest()
                .forFile("80D1B8B90CED30430608C58002811B3285FC")
                .withPath("/public/test")
                .withFileAttributes(attributes().size(10, KiB).storageInfo(aStorageInfo()))
                .withProtocolInfo(aProtocolInfo().withProtocol("http")
                        .withMajorVersion(1).withIPAddress("192.168.1.1")));

        var reply = replySentWith(endpoint);
        then(reply).should().setFailed(eq(10018), any());
        then(reply).should().setContext(1, null);
        then(billing).shouldHaveNoInteractions();
    }

    @Test
    public void shouldStageReadRequestForTapeFileWhenReadAndP2pTriggerPermissionDenied() throws Exception
    {
        var stagePool = aPool("stage-pool@dCacheDomain");
        given(aPartitionManager().withDefault(aPartition().withStageAllowed(true).withP2pAllowed(true)));
        given(aPoolSelectionUnit().withNetUnit("all-net", "192.168.1.1")
                .withProtocolUnit("HTTP", "http/1"));
        given(aPoolMonitor().thatReturns(aPoolSelectorThat()
                .onReadThrows(aPermissionDeniedCacheException())
                .onPool2PoolThrows(aPermissionDeniedCacheException())
                .onStageSelects(stagePool)));
        given(aCell("stage-pool@dCacheDomain").communicatingVia(endpoint)
                .thatOnReceiving(PoolFetchFileMessage.class).repliesWithSuccess());
        given(aContainer("PoolManager@dCacheDomain")
                .thatDoesNotSendHitMessages());

        whenReceiving(aReadRequest()
                .by(ROOT)
                .forFile("80D1B8B90CED30430608C58002811B3285FC")
                .withPath("/public/test")
                .withFileAttributes(attributes().size(10, KiB)
                        .storageInfo(aStorageInfo().withLocation("osm://RZ1/bfid1")))
                .withProtocolInfo(aProtocolInfo().withProtocol("http")
                        .withMajorVersion(1).withIPAddress("192.168.1.1")));

        var reply = replySentWith(endpoint);
        then(reply).should().setSucceeded();
        then(reply).should().setContext(1, stagePool);
        var selectedPool = poolSetInMessage(reply);
        assertThat(selectedPool.getName(), equalTo("stage-pool"));
        assertThat(selectedPool.getAddress(), equalTo(new CellAddressCore("stage-pool@dCacheDomain")));
        then(billing).shouldHaveNoInteractions();
    }

    @Test
    public void shouldSuspendReadRequestForNonTapeFileWhenReadAndP2pTriggerPermissionDenied() throws Exception
    {
        var storageInfo = aStorageInfo().build();
        given(aPartitionManager().withDefault(aPartition().withStageAllowed(true).withP2pAllowed(true)));
        given(aPoolSelectionUnit().withNetUnit("all-net", "192.168.1.1")
                .withProtocolUnit("HTTP", "http/1"));
        given(aPoolMonitor().thatReturns(aPoolSelectorThat()
                .onReadThrows(aPermissionDeniedCacheException())
                .onPool2PoolThrows(aPermissionDeniedCacheException())));
        given(aContainer("PoolManager@dCacheDomain")
                .thatDoesNotSendHitMessages()
                .withConfig("rc onerror suspend"));

        whenReceiving(aReadRequest()
                .forFile("80D1B8B90CED30430608C58002811B3285FC")
                .withBillingPath("/public/test")
                .withTransferPath("/uploads/50/test")
                .withFileAttributes(attributes().size(10, KiB)
                        .storageInfo(storageInfo))
                .withProtocolInfo(aProtocolInfo().withProtocol("http")
                        .withMajorVersion(1).withIPAddress("192.168.1.1")));

        then(endpoint).shouldHaveNoInteractions();

        var warning = notificationSentWith(billing, WarningPnfsFileInfoMessage.class);
        assertThat(warning.getResultCode(), equalTo(0));
        assertThat(warning.getCellAddress(), equalTo(new CellAddressCore("PoolManager@dCacheDomain")));
        assertThat(warning.getBillingPath(), equalTo("/public/test"));
        assertThat(warning.getTransferPath(), equalTo("/uploads/50/test"));
        assertThat(warning.getFileSize(), equalTo(KiB.toBytes(10L)));
        assertThat(warning.getStorageInfo(), is(storageInfo));

        whenExecutedAdminCommand("rc retry * -force-all");

        var reply = replySentWith(endpoint);
        then(reply).should().setFailed(eq(10021), any());
        then(reply).should().setContext(0, null);
        then(billing).shouldHaveNoMoreInteractions();
    }

    @Test
    public void shouldFailReadRequestForNonTapeFileWhenReadAndP2pTriggerPermissionDenied() throws Exception
    {
        given(aPartitionManager().withDefault(aPartition().withStageAllowed(true).withP2pAllowed(true)));
        given(aPoolSelectionUnit().withNetUnit("all-net", "192.168.1.1")
                .withProtocolUnit("HTTP", "http/1"));
        given(aPoolMonitor().thatReturns(aPoolSelectorThat()
                .onReadThrows(aPermissionDeniedCacheException())
                .onPool2PoolThrows(aPermissionDeniedCacheException())));
        given(aContainer("PoolManager@dCacheDomain")
                .thatDoesNotSendHitMessages()
                .withConfig("rc onerror fail"));

        whenReceiving(aReadRequest()
                .forFile("80D1B8B90CED30430608C58002811B3285FC")
                .withBillingPath("/public/test")
                .withTransferPath("/uploads/50/test")
                .withFileAttributes(attributes().size(10, KiB)
                        .storageInfo(aStorageInfo()))
                .withProtocolInfo(aProtocolInfo().withProtocol("http")
                        .withMajorVersion(1).withIPAddress("192.168.1.1")));

        var reply = replySentWith(endpoint);
        then(reply).should().setFailed(eq(265), any());
        then(reply).should().setContext(1, null);
        then(billing).shouldHaveNoMoreInteractions();
    }


    @Test
    public void shouldSuspendReadRequestWhenReadAndP2pTriggerPermissionDeniedAndNoHsm() throws Exception
    {
        var storageInfo = aStorageInfo().build();
        given(aPartitionManager().withDefault(aPartition().withStageAllowed(false).withP2pAllowed(true)));
        given(aPoolSelectionUnit().withNetUnit("all-net", "192.168.1.1")
                .withProtocolUnit("HTTP", "http/1"));
        given(aPoolMonitor().thatReturns(aPoolSelectorThat()
                .onReadThrows(aPermissionDeniedCacheException())
                .onPool2PoolThrows(aPermissionDeniedCacheException())));
        given(aContainer("PoolManager@dCacheDomain")
                .thatDoesNotSendHitMessages()
                .withConfig("rc onerror suspend"));

        whenReceiving(aReadRequest()
                .forFile("80D1B8B90CED30430608C58002811B3285FC")
                .withBillingPath("/public/test")
                .withTransferPath("/uploads/50/test")
                .withFileAttributes(attributes().size(10, KiB)
                        .storageInfo(storageInfo))
                .withProtocolInfo(aProtocolInfo().withProtocol("http")
                        .withMajorVersion(1).withIPAddress("192.168.1.1")));

        then(endpoint).shouldHaveNoInteractions();

        var warning = notificationSentWith(billing, WarningPnfsFileInfoMessage.class);
        assertThat(warning.getResultCode(), equalTo(0));
        assertThat(warning.getCellAddress(), equalTo(new CellAddressCore("PoolManager@dCacheDomain")));
        assertThat(warning.getBillingPath(), equalTo("/public/test"));
        assertThat(warning.getTransferPath(), equalTo("/uploads/50/test"));
        assertThat(warning.getFileSize(), equalTo(KiB.toBytes(10L)));
        assertThat(warning.getStorageInfo(), is(storageInfo));

        whenExecutedAdminCommand("rc retry * -force-all");

        var reply = replySentWith(endpoint);
        then(reply).should().setFailed(eq(10021), any());
        then(reply).should().setContext(0, null);
        then(billing).shouldHaveNoMoreInteractions();
    }

    @Test
    public void shouldFailReadRequestWhenReadAndP2pTriggerPermissionDeniedAndNoHsm() throws Exception
    {
        given(aPartitionManager().withDefault(aPartition().withStageAllowed(false).withP2pAllowed(true)));
        given(aPoolSelectionUnit().withNetUnit("all-net", "192.168.1.1")
                .withProtocolUnit("HTTP", "http/1"));
        given(aPoolMonitor().thatReturns(aPoolSelectorThat()
                .onReadThrows(aPermissionDeniedCacheException())
                .onPool2PoolThrows(aPermissionDeniedCacheException())));
        given(aContainer("PoolManager@dCacheDomain")
                .thatDoesNotSendHitMessages()
                .withConfig("rc onerror fail"));

        whenReceiving(aReadRequest()
                .forFile("80D1B8B90CED30430608C58002811B3285FC")
                .withBillingPath("/public/test")
                .withTransferPath("/uploads/50/test")
                .withFileAttributes(attributes().size(10, KiB)
                        .storageInfo(aStorageInfo()))
                .withProtocolInfo(aProtocolInfo().withProtocol("http")
                        .withMajorVersion(1).withIPAddress("192.168.1.1")));

        var reply = replySentWith(endpoint);
        then(reply).should().setFailed(eq(265), any());
        then(reply).should().setContext(1, null);
        then(billing).shouldHaveNoMoreInteractions();
    }

    @Test
    public void shouldFallBackToHotPoolIfCostExceededWithPool2PoolOnCostAndPool2PoolPermissionDenied() throws Exception
    {
        given(aPartitionManager().withDefault(aPartition()));
        given(aPoolSelectionUnit().withNetUnit("all-net", "192.168.1.1")
                .withProtocolUnit("HTTP", "http/1"));
        given(aPoolMonitor().thatReturns(aPoolSelectorThat()
                .returnsCurrentPartition(aPartition().withP2pOnCost(true))
                .onReadThrows(aCostException()
                        .withPool("hot-pool@dCacheDomain")
                        .withTryAlternatives(true))
                .onPool2PoolThrows(aPermissionDeniedCacheException())));
        given(aContainer("PoolManager@dCacheDomain").thatDoesNotSendHitMessages());

        whenReceiving(aReadRequest()
                .forFile("80D1B8B90CED30430608C58002811B3285FC")
                .withBillingPath("/public/test")
                .withTransferPath("/uploads/50/test")
                .withFileAttributes(attributes().size(10, KiB).storageInfo(aStorageInfo()))
                .withProtocolInfo(aProtocolInfo().withProtocol("http")
                        .withMajorVersion(1).withIPAddress("192.168.1.1")));

        var reply = replySentWith(endpoint);
        then(reply).should().setSucceeded();
        then(reply).should().setContext(1, null);
        var selectedPool = poolSetInMessage(reply);
        assertThat(selectedPool.getName(), equalTo("hot-pool"));
        assertThat(selectedPool.getAddress(), equalTo(new CellAddressCore("hot-pool@dCacheDomain")));
        then(billing).shouldHaveNoInteractions();
    }

    @Test
    public void shouldFailReplicateRequestIfSourceCostExceededPool2PoolAndStageOnCost() throws Exception
    {
        given(aPartitionManager().withDefault(aPartition().withP2pOnCost(true).withStageAllowed(true).withStageOnCost(true)));
        given(aPoolSelectionUnit().withNetUnit("all-net", "192.168.1.1")
                .withProtocolUnit("HTTP", "http/1"));
        given(aPoolMonitor().thatReturns(aPoolSelectorThat()
                .onPool2PoolThrows(aSourceCostException())));
        given(aContainer("PoolManager@dCacheDomain").thatDoesNotSendHitMessages());

        whenReceiving(aReplicateRequest()
                .forFile("80D1B8B90CED30430608C58002811B3285FC")
                .withBillingPath("/public/test")
                .withTransferPath("/uploads/50/test")
                .withFileAttributes(attributes().size(10, KiB)
                        .storageInfo(aStorageInfo().withLocation("osm://RZ1/bfid1")))
                .withProtocolInfo(aProtocolInfo().withProtocol("http")
                        .withMajorVersion(1).withIPAddress("192.168.1.1")));

        var reply = replySentWith(endpoint);
        then(reply).should().setFailed(eq(10017), any());
        then(reply).should().setContext(1, null);
        then(billing).shouldHaveNoInteractions();
    }

    @Test
    public void shouldStageReadRequestIfSourceCostExceededPool2PoolAndStageOnCost() throws Exception
    {
        var stagePool = aPool("stage-pool@dCacheDomain");
        given(aPartitionManager().withDefault(aPartition()));
        given(aPoolSelectionUnit().withNetUnit("all-net", "192.168.1.1")
                .withProtocolUnit("HTTP", "http/1"));
        given(aPoolMonitor().thatReturns(aPoolSelectorThat()
                .onReadThrows(aCostException()
                        .withPool("hot-pool@dCacheDomain")
                        .withTryAlternatives(true))
                .returnsCurrentPartition(aPartition().withStageAllowed(true).withStageOnCost(true).withP2pOnCost(true))
                .onPool2PoolThrows(aSourceCostException())
                .onStageSelects(stagePool)));
        given(aCell("stage-pool@dCacheDomain").communicatingVia(endpoint)
                .thatOnReceiving(PoolFetchFileMessage.class).repliesWithSuccess());
        given(aContainer("PoolManager@dCacheDomain").thatDoesNotSendHitMessages());

        whenReceiving(aReadRequest()
                .by(ROOT)
                .forFile("80D1B8B90CED30430608C58002811B3285FC")
                .withBillingPath("/public/test")
                .withTransferPath("/uploads/50/test")
                .withFileAttributes(attributes().size(10, KiB)
                        .storageInfo(aStorageInfo().withLocation("osm://RZ1/bfid1")))
                .withProtocolInfo(aProtocolInfo().withProtocol("http")
                        .withMajorVersion(1).withIPAddress("192.168.1.1")));

        var reply = replySentWith(endpoint);
        then(reply).should().setSucceeded();
        then(reply).should().setContext(1, stagePool);
        var selectedPool = poolSetInMessage(reply);
        assertThat(selectedPool.getName(), equalTo("stage-pool"));
        assertThat(selectedPool.getAddress(), equalTo(new CellAddressCore("stage-pool@dCacheDomain")));
        then(billing).shouldHaveNoInteractions();
    }

    @Test
    public void shouldFallBackToHotPoolIfSourceCostExceededForNonTapeFile() throws Exception
    {
        given(aPartitionManager().withDefault(aPartition()));
        given(aPoolSelectionUnit().withNetUnit("all-net", "192.168.1.1")
                .withProtocolUnit("HTTP", "http/1"));
        given(aPoolMonitor().thatReturns(aPoolSelectorThat()
                .onReadThrows(aCostException()
                        .withPool("hot-pool@dCacheDomain")
                        .withTryAlternatives(true))
                .returnsCurrentPartition(aPartition().withStageAllowed(true).withStageOnCost(true).withP2pOnCost(true))
                .onPool2PoolThrows(aSourceCostException())));
        given(aContainer("PoolManager@dCacheDomain").thatDoesNotSendHitMessages());

        whenReceiving(aReadRequest()
                .forFile("80D1B8B90CED30430608C58002811B3285FC")
                .withBillingPath("/public/test")
                .withTransferPath("/uploads/50/test")
                .withFileAttributes(attributes().size(10, KiB)
                        .storageInfo(aStorageInfo()))
                .withProtocolInfo(aProtocolInfo().withProtocol("http")
                        .withMajorVersion(1).withIPAddress("192.168.1.1")));

        var reply = replySentWith(endpoint);
        then(reply).should().setSucceeded();
        then(reply).should().setContext(1, null);
        var selectedPool = poolSetInMessage(reply);
        assertThat(selectedPool.getName(), equalTo("hot-pool"));
        assertThat(selectedPool.getAddress(), equalTo(new CellAddressCore("hot-pool@dCacheDomain")));
        then(billing).shouldHaveNoInteractions();
    }

    @Test
    public void shouldFallBackToHotPoolIfSourceCostExceededAndNotStageOnCost() throws Exception
    {
        given(aPartitionManager().withDefault(aPartition()));
        given(aPoolSelectionUnit().withNetUnit("all-net", "192.168.1.1")
                .withProtocolUnit("HTTP", "http/1"));
        given(aPoolMonitor().thatReturns(aPoolSelectorThat()
                .onReadThrows(aCostException()
                        .withPool("hot-pool@dCacheDomain")
                        .withTryAlternatives(true))
                .returnsCurrentPartition(aPartition().withStageAllowed(true).withStageOnCost(false).withP2pOnCost(true))
                .onPool2PoolThrows(aSourceCostException())));
        given(aContainer("PoolManager@dCacheDomain").thatDoesNotSendHitMessages());

        whenReceiving(aReadRequest()
                .forFile("80D1B8B90CED30430608C58002811B3285FC")
                .withBillingPath("/public/test")
                .withTransferPath("/uploads/50/test")
                .withFileAttributes(attributes().size(10, KiB)
                        .storageInfo(aStorageInfo().withLocation("osm://RZ1/bfid1")))
                .withProtocolInfo(aProtocolInfo().withProtocol("http")
                        .withMajorVersion(1).withIPAddress("192.168.1.1")));

        var reply = replySentWith(endpoint);
        then(reply).should().setSucceeded();
        then(reply).should().setContext(1, null);
        var selectedPool = poolSetInMessage(reply);
        assertThat(selectedPool.getName(), equalTo("hot-pool"));
        assertThat(selectedPool.getAddress(), equalTo(new CellAddressCore("hot-pool@dCacheDomain")));
        then(billing).shouldHaveNoInteractions();
    }

    @Test
    public void shouldFallBackToHotPoolIfSourceCostExceededAndNoHsm() throws Exception
    {
        given(aPartitionManager().withDefault(aPartition()));
        given(aPoolSelectionUnit().withNetUnit("all-net", "192.168.1.1")
                .withProtocolUnit("HTTP", "http/1"));
        given(aPoolMonitor().thatReturns(aPoolSelectorThat()
                .onReadThrows(aCostException()
                        .withPool("hot-pool@dCacheDomain")
                        .withTryAlternatives(true))
                .returnsCurrentPartition(aPartition().withStageAllowed(false).withStageOnCost(true).withP2pOnCost(true))
                .onPool2PoolThrows(aSourceCostException())));
        given(aContainer("PoolManager@dCacheDomain").thatDoesNotSendHitMessages());

        whenReceiving(aReadRequest()
                .forFile("80D1B8B90CED30430608C58002811B3285FC")
                .withBillingPath("/public/test")
                .withTransferPath("/uploads/50/test")
                .withFileAttributes(attributes().size(10, KiB)
                        .storageInfo(aStorageInfo().withLocation("osm://RZ1/bfid1")))
                .withProtocolInfo(aProtocolInfo().withProtocol("http")
                        .withMajorVersion(1).withIPAddress("192.168.1.1")));

        var reply = replySentWith(endpoint);
        then(reply).should().setSucceeded();
        then(reply).should().setContext(1, null);
        var selectedPool = poolSetInMessage(reply);
        assertThat(selectedPool.getName(), equalTo("hot-pool"));
        assertThat(selectedPool.getAddress(), equalTo(new CellAddressCore("hot-pool@dCacheDomain")));
        then(billing).shouldHaveNoInteractions();
    }

    @Test
    public void shouldFailIfSourceCostExceededAndNoFallback() throws Exception
    {
        given(aPartitionManager().withDefault(aPartition().withP2pAllowed(true)));
        given(aPoolSelectionUnit().withNetUnit("all-net", "192.168.1.1")
                .withProtocolUnit("HTTP", "http/1"));
        given(aPoolMonitor().thatReturns(aPoolSelectorThat()
                .onReadThrows(aPermissionDeniedCacheException())
                .onPool2PoolThrows(aSourceCostException())));
        given(aContainer("PoolManager@dCacheDomain").thatDoesNotSendHitMessages());

        whenReceiving(aReadRequest()
                .forFile("80D1B8B90CED30430608C58002811B3285FC")
                .withBillingPath("/public/test")
                .withTransferPath("/uploads/50/test")
                .withFileAttributes(attributes().size(10, KiB)
                        .storageInfo(aStorageInfo()))
                .withProtocolInfo(aProtocolInfo().withProtocol("http")
                        .withMajorVersion(1).withIPAddress("192.168.1.1")));

        var reply = replySentWith(endpoint);
        then(reply).should().setFailed(eq(194), any());
        then(reply).should().setContext(1, null);
        then(billing).shouldHaveNoInteractions();
    }

    @Test
    public void shouldFailReplicateRequestIfDestinationCostExceededPool2PoolAndStageOnCost() throws Exception
    {
        given(aPartitionManager().withDefault(aPartition().withP2pOnCost(true).withStageAllowed(true).withStageOnCost(true)));
        given(aPoolSelectionUnit().withNetUnit("all-net", "192.168.1.1")
                .withProtocolUnit("HTTP", "http/1"));
        given(aPoolMonitor().thatReturns(aPoolSelectorThat()
                .onPool2PoolThrows(aDestinationCostException())));
        given(aContainer("PoolManager@dCacheDomain").thatDoesNotSendHitMessages());

        whenReceiving(aReplicateRequest()
                .forFile("80D1B8B90CED30430608C58002811B3285FC")
                .withBillingPath("/public/test")
                .withTransferPath("/uploads/50/test")
                .withFileAttributes(attributes().size(10, KiB)
                        .storageInfo(aStorageInfo().withLocation("osm://RZ1/bfid1")))
                .withProtocolInfo(aProtocolInfo().withProtocol("http")
                        .withMajorVersion(1).withIPAddress("192.168.1.1")));

        var reply = replySentWith(endpoint);
        then(reply).should().setFailed(eq(10017), any());
        then(reply).should().setContext(1, null);
        then(billing).shouldHaveNoInteractions();
    }

    @Test
    public void shouldFailIfDestinationCostExceededAndNoFallback() throws Exception
    {
        given(aPartitionManager().withDefault(aPartition().withP2pAllowed(true)));
        given(aPoolSelectionUnit().withNetUnit("all-net", "192.168.1.1")
                .withProtocolUnit("HTTP", "http/1"));
        given(aPoolMonitor().thatReturns(aPoolSelectorThat()
                .onReadThrows(aPermissionDeniedCacheException())
                .onPool2PoolThrows(aDestinationCostException())));
        given(aContainer("PoolManager@dCacheDomain").thatDoesNotSendHitMessages());

        whenReceiving(aReadRequest()
                .forFile("80D1B8B90CED30430608C58002811B3285FC")
                .withBillingPath("/public/test")
                .withTransferPath("/uploads/50/test")
                .withFileAttributes(attributes().size(10, KiB)
                        .storageInfo(aStorageInfo()))
                .withProtocolInfo(aProtocolInfo().withProtocol("http")
                        .withMajorVersion(1).withIPAddress("192.168.1.1")));

        var reply = replySentWith(endpoint);
        then(reply).should().setFailed(eq(192), any());
        then(reply).should().setContext(1, null);
        then(billing).shouldHaveNoInteractions();
    }

    @Test
    public void shouldFallBackToHotPoolIfDestinationCostExceeded() throws Exception
    {
        given(aPartitionManager().withDefault(aPartition()));
        given(aPoolSelectionUnit().withNetUnit("all-net", "192.168.1.1")
                .withProtocolUnit("HTTP", "http/1"));
        given(aPoolMonitor().thatReturns(aPoolSelectorThat()
                .onReadThrows(aCostException()
                        .withPool("hot-pool@dCacheDomain")
                        .withTryAlternatives(true))
                .returnsCurrentPartition(aPartition().withP2pOnCost(true))
                .onPool2PoolThrows(aDestinationCostException())));
        given(aContainer("PoolManager@dCacheDomain").thatDoesNotSendHitMessages());

        whenReceiving(aReadRequest()
                .forFile("80D1B8B90CED30430608C58002811B3285FC")
                .withBillingPath("/public/test")
                .withTransferPath("/uploads/50/test")
                .withFileAttributes(attributes().size(10, KiB)
                        .storageInfo(aStorageInfo().withLocation("osm://RZ1/bfid1")))
                .withProtocolInfo(aProtocolInfo().withProtocol("http")
                        .withMajorVersion(1).withIPAddress("192.168.1.1")));

        var reply = replySentWith(endpoint);
        then(reply).should().setSucceeded();
        then(reply).should().setContext(1, null);
        var selectedPool = poolSetInMessage(reply);
        assertThat(selectedPool.getName(), equalTo("hot-pool"));
        assertThat(selectedPool.getAddress(), equalTo(new CellAddressCore("hot-pool@dCacheDomain")));
        then(billing).shouldHaveNoInteractions();
    }

    @Test
    public void shouldFailReplicateRequestForPool2PoolCacheException() throws Exception
    {
        given(aPartitionManager().withDefault(aPartition().withP2pOnCost(true)));
        given(aPoolSelectionUnit().withNetUnit("all-net", "192.168.1.1")
                .withProtocolUnit("HTTP", "http/1"));
        given(aPoolMonitor().thatReturns(aPoolSelectorThat()
                .onPool2PoolThrows(aCacheException(3141))));
        given(aContainer("PoolManager@dCacheDomain").thatDoesNotSendHitMessages());

        whenReceiving(aReplicateRequest()
                .forFile("80D1B8B90CED30430608C58002811B3285FC")
                .withBillingPath("/public/test")
                .withTransferPath("/uploads/50/test")
                .withFileAttributes(attributes().size(10, KiB)
                        .storageInfo(aStorageInfo().withLocation("osm://RZ1/bfid1")))
                .withProtocolInfo(aProtocolInfo().withProtocol("http")
                        .withMajorVersion(1).withIPAddress("192.168.1.1")));

        var reply = replySentWith(endpoint);
        then(reply).should().setFailed(eq(3141), any());
        then(reply).should().setContext(1, null);
        then(billing).shouldHaveNoInteractions();
    }

    @Test
    public void shouldFailReplicateRequestForPool2PoolWithIllegalArgumentException() throws Exception
    {
        given(aPartitionManager().withDefault(aPartition().withP2pOnCost(true)));
        given(aPoolSelectionUnit().withNetUnit("all-net", "192.168.1.1")
                .withProtocolUnit("HTTP", "http/1"));
        given(aPoolMonitor().thatReturns(aPoolSelectorThat()
                .onPool2PoolThrows(anIllegalArgumentException())));
        given(aContainer("PoolManager@dCacheDomain").thatDoesNotSendHitMessages());

        whenReceiving(aReplicateRequest()
                .forFile("80D1B8B90CED30430608C58002811B3285FC")
                .withBillingPath("/public/test")
                .withTransferPath("/uploads/50/test")
                .withFileAttributes(attributes().size(10, KiB)
                        .storageInfo(aStorageInfo().withLocation("osm://RZ1/bfid1")))
                .withProtocolInfo(aProtocolInfo().withProtocol("http")
                        .withMajorVersion(1).withIPAddress("192.168.1.1")));

        var reply = replySentWith(endpoint);
        then(reply).should().setFailed(eq(128), any());
        then(reply).should().setContext(1, null);
        then(billing).shouldHaveNoInteractions();
    }

    @Test
    public void shouldFailReplicateRequestForPool2PoolWithRuntimeException() throws Exception
    {
        given(aPartitionManager().withDefault(aPartition().withP2pOnCost(true)));
        given(aPoolSelectionUnit().withNetUnit("all-net", "192.168.1.1")
                .withProtocolUnit("HTTP", "http/1"));
        given(aPoolMonitor().thatReturns(aPoolSelectorThat()
                .onPool2PoolThrows(aRuntimeException())));
        given(aContainer("PoolManager@dCacheDomain").thatDoesNotSendHitMessages());

        whenReceiving(aReplicateRequest()
                .forFile("80D1B8B90CED30430608C58002811B3285FC")
                .withBillingPath("/public/test")
                .withTransferPath("/uploads/50/test")
                .withFileAttributes(attributes().size(10, KiB)
                        .storageInfo(aStorageInfo().withLocation("osm://RZ1/bfid1")))
                .withProtocolInfo(aProtocolInfo().withProtocol("http")
                        .withMajorVersion(1).withIPAddress("192.168.1.1")));

        var reply = replySentWith(endpoint);
        // With dCache v7.1, bugs in PoolSelector no longer handled specially.
        // Therefore, there door receives a "generic bug" return code (10011)
        // instead of a PoolSelector-specific bug return code (128).
        then(reply).should().setFailed(eq(10011), any());
        then(reply).should().setContext(1, null);
        then(billing).shouldHaveNoInteractions();
    }

    @Test
    public void shouldStageReadRequestIfPool2PoolWithCacheException() throws Exception
    {
        var stagePool = aPool("stage-pool@dCacheDomain");
        given(aPartitionManager().withDefault(aPartition()));
        given(aPoolSelectionUnit().withNetUnit("all-net", "192.168.1.1")
                .withProtocolUnit("HTTP", "http/1"));
        given(aPoolMonitor().thatReturns(aPoolSelectorThat()
                .onReadThrows(aCostException()
                        .withPool("hot-pool@dCacheDomain")
                        .withTryAlternatives(true))
                .returnsCurrentPartition(aPartition().withStageAllowed(true).withStageOnCost(true).withP2pOnCost(true))
                .onPool2PoolThrows(aCacheException())
                .onStageSelects(stagePool)));
        given(aCell("stage-pool@dCacheDomain").communicatingVia(endpoint)
                .thatOnReceiving(PoolFetchFileMessage.class).repliesWithSuccess());
        given(aContainer("PoolManager@dCacheDomain").thatDoesNotSendHitMessages());

        whenReceiving(aReadRequest()
                .by(ROOT)
                .forFile("80D1B8B90CED30430608C58002811B3285FC")
                .withBillingPath("/public/test")
                .withTransferPath("/uploads/50/test")
                .withFileAttributes(attributes().size(10, KiB)
                        .storageInfo(aStorageInfo().withLocation("osm://RZ1/bfid1")))
                .withProtocolInfo(aProtocolInfo().withProtocol("http")
                        .withMajorVersion(1).withIPAddress("192.168.1.1")));

        var reply = replySentWith(endpoint);
        then(reply).should().setSucceeded();
        then(reply).should().setContext(1, stagePool);
        var selectedPool = poolSetInMessage(reply);
        assertThat(selectedPool.getName(), equalTo("stage-pool"));
        assertThat(selectedPool.getAddress(), equalTo(new CellAddressCore("stage-pool@dCacheDomain")));
        then(billing).shouldHaveNoInteractions();
    }

    @Test
    public void shouldFailReadRequestForNonTapeFileIfPool2PoolWithCacheException() throws Exception
    {
        given(aPartitionManager().withDefault(aPartition()));
        given(aPoolSelectionUnit().withNetUnit("all-net", "192.168.1.1")
                .withProtocolUnit("HTTP", "http/1"));
        given(aPoolMonitor().thatReturns(aPoolSelectorThat()
                .onReadThrows(aCostException()
                        .withPool("hot-pool@dCacheDomain")
                        .withTryAlternatives(true))
                .returnsCurrentPartition(aPartition().withStageAllowed(true).withP2pOnCost(true))
                .onPool2PoolThrows(aCacheException(3141))));
        given(aContainer("PoolManager@dCacheDomain")
                .thatDoesNotSendHitMessages()
                .withConfig("rc onerror fail"));

        whenReceiving(aReadRequest()
                .by(ROOT)
                .forFile("80D1B8B90CED30430608C58002811B3285FC")
                .withBillingPath("/public/test")
                .withTransferPath("/uploads/50/test")
                .withFileAttributes(attributes().size(10, KiB)
                        .storageInfo(aStorageInfo()))
                .withProtocolInfo(aProtocolInfo().withProtocol("http")
                        .withMajorVersion(1).withIPAddress("192.168.1.1")));

        var reply = replySentWith(endpoint);
        then(reply).should().setFailed(eq(3141), any());
        then(reply).should().setContext(1, null);
        then(billing).shouldHaveNoInteractions();
    }

    @Test
    public void shouldFailReadRequestIfPool2PoolWithCacheExceptionWithoutHsm() throws Exception
    {
        given(aPartitionManager().withDefault(aPartition()));
        given(aPoolSelectionUnit().withNetUnit("all-net", "192.168.1.1")
                .withProtocolUnit("HTTP", "http/1"));
        given(aPoolMonitor().thatReturns(aPoolSelectorThat()
                .onReadThrows(aCostException()
                        .withPool("hot-pool@dCacheDomain")
                        .withTryAlternatives(true))
                .returnsCurrentPartition(aPartition().withStageAllowed(false).withP2pOnCost(true))
                .onPool2PoolThrows(aCacheException(3141))));
        given(aContainer("PoolManager@dCacheDomain")
                .thatDoesNotSendHitMessages()
                .withConfig("rc onerror fail"));

        whenReceiving(aReadRequest()
                .by(ROOT)
                .forFile("80D1B8B90CED30430608C58002811B3285FC")
                .withBillingPath("/public/test")
                .withTransferPath("/uploads/50/test")
                .withFileAttributes(attributes().size(10, KiB)
                        .storageInfo(aStorageInfo()))
                .withProtocolInfo(aProtocolInfo().withProtocol("http")
                        .withMajorVersion(1).withIPAddress("192.168.1.1")));

        var reply = replySentWith(endpoint);
        then(reply).should().setFailed(eq(3141), any());
        then(reply).should().setContext(1, null);
        then(billing).shouldHaveNoInteractions();
    }

    @Test
    public void shouldSuspendReadRequestForNonTapeFileIfPool2PoolWithCacheException() throws Exception
    {
        var storageInfo = aStorageInfo().build();
        given(aPartitionManager().withDefault(aPartition()));
        given(aPoolSelectionUnit().withNetUnit("all-net", "192.168.1.1")
                .withProtocolUnit("HTTP", "http/1"));
        given(aPoolMonitor().thatReturns(aPoolSelectorThat()
                .onReadThrows(aCostException()
                        .withPool("hot-pool@dCacheDomain")
                        .withTryAlternatives(true))
                .returnsCurrentPartition(aPartition().withStageAllowed(true).withP2pOnCost(true))
                .onPool2PoolThrows(aCacheException())));
        given(aContainer("PoolManager@dCacheDomain")
                .thatDoesNotSendHitMessages()
                .withConfig("rc onerror suspend"));

        whenReceiving(aReadRequest()
                .by(ROOT)
                .forFile("80D1B8B90CED30430608C58002811B3285FC")
                .withBillingPath("/public/test")
                .withTransferPath("/uploads/50/test")
                .withFileAttributes(attributes().size(10, KiB)
                        .storageInfo(storageInfo))
                .withProtocolInfo(aProtocolInfo().withProtocol("http")
                        .withMajorVersion(1).withIPAddress("192.168.1.1")));

        then(endpoint).shouldHaveNoInteractions();

        var warning = notificationSentWith(billing, WarningPnfsFileInfoMessage.class);
        assertThat(warning.getResultCode(), equalTo(0));
        assertThat(warning.getCellAddress(), equalTo(new CellAddressCore("PoolManager@dCacheDomain")));
        assertThat(warning.getBillingPath(), equalTo("/public/test"));
        assertThat(warning.getTransferPath(), equalTo("/uploads/50/test"));
        assertThat(warning.getFileSize(), equalTo(KiB.toBytes(10L)));
        assertThat(warning.getStorageInfo(), is(storageInfo));

        whenExecutedAdminCommand("rc retry * -force-all");

        var reply = replySentWith(endpoint);
        then(reply).should().setFailed(eq(10021), any());
        then(reply).should().setContext(0, null);
        then(billing).shouldHaveNoMoreInteractions();
    }

    @Test
    public void shouldReturnRetryForReadRequestTriggeringSuccessfulPool2PoolWithP2pForTransfer() throws Exception
    {
        given(aPartitionManager().withDefault(aPartition().withP2pAllowed(true).withP2pForTransfer(true)));
        given(aPoolSelectionUnit().withNetUnit("all-net", "192.168.1.1")
                .withProtocolUnit("HTTP", "http/1"));
        given(aPoolMonitor().thatReturns(aPoolSelectorThat()
                .onReadThrows(aPermissionDeniedCacheException())
                .onPool2PoolSelects(aPoolPair()
                        .withSource("source-pool@dCacheDomain")
                        .withDestination("destination-pool@dCacheDomain"))));
        given(aCell("destination-pool@dCacheDomain").communicatingVia(endpoint)
                .thatOnReceiving(Pool2PoolTransferMsg.class).repliesWithSuccess());
        given(aContainer("PoolManager@dCacheDomain").thatDoesNotSendHitMessages());

        whenReceiving(aReadRequest()
                .forFile("80D1B8B90CED30430608C58002811B3285FC")
                .withBillingPath("/public/test")
                .withTransferPath("/uploads/50/test")
                .withFileAttributes(attributes().size(10, KiB)
                        .storageInfo(aStorageInfo()))
                .withProtocolInfo(aProtocolInfo().withProtocol("http")
                        .withMajorVersion(1).withIPAddress("192.168.1.1")));

        var reply = replySentWith(endpoint);
        then(reply).should().setFailed(eq(10021), any());
        then(reply).should().setContext(1, null);
        then(billing).shouldHaveNoMoreInteractions();
    }

    @Test
    public void shouldSucceedForReplicateRequestTriggeringSuccessfulPool2PoolWithP2pForTransfer() throws Exception
    {
        given(aPartitionManager().withDefault(aPartition().withP2pAllowed(true).withP2pForTransfer(true)));
        given(aPoolSelectionUnit().withNetUnit("all-net", "192.168.1.1")
                .withProtocolUnit("HTTP", "http/1"));
        given(aPoolMonitor().thatReturns(aPoolSelectorThat()
                .onPool2PoolSelects(aPoolPair()
                        .withSource("source-pool@dCacheDomain")
                        .withDestination("destination-pool@dCacheDomain"))));
        given(aCell("destination-pool@dCacheDomain").communicatingVia(endpoint)
                .thatOnReceiving(Pool2PoolTransferMsg.class).repliesWithSuccess());
        given(aContainer("PoolManager@dCacheDomain").thatDoesNotSendHitMessages());

        whenReceiving(aReplicateRequest()
                .forFile("80D1B8B90CED30430608C58002811B3285FC")
                .withPath("/public/test")
                .withFileAttributes(attributes().size(10, KiB).storageInfo(aStorageInfo()))
                .withProtocolInfo(aProtocolInfo().withProtocol("http")
                        .withMajorVersion(1).withIPAddress("192.168.1.1")));

        var reply = replySentWith(endpoint);
        then(reply).should().setSucceeded();
        then(reply).should().setContext(1, null);
        var selectedPool = poolSetInMessage(reply);
        assertThat(selectedPool.getName(), equalTo("destination-pool"));
        assertThat(selectedPool.getAddress(), equalTo(new CellAddressCore("destination-pool@dCacheDomain")));
        then(billing).shouldHaveNoInteractions();
    }

    @Test
    public void shouldFailReadRequestTriggeringFailedPool2PoolWithNoHsm() throws Exception
    {
        given(aPartitionManager().withDefault(aPartition().withP2pAllowed(true).withStageAllowed(false)));
        given(aPoolSelectionUnit().withNetUnit("all-net", "192.168.1.1")
                .withProtocolUnit("HTTP", "http/1"));
        given(aPoolMonitor().thatReturns(aPoolSelectorThat()
                .onReadThrows(aPermissionDeniedCacheException())
                .onPool2PoolSelects(aPoolPair()
                        .withSource("source-pool@dCacheDomain")
                        .withDestination("destination-pool@dCacheDomain"))));
        given(aCell("destination-pool@dCacheDomain").communicatingVia(endpoint)
                .thatOnReceiving(Pool2PoolTransferMsg.class).repliesWithReturnCode(3141));
        given(aContainer("PoolManager@dCacheDomain").thatDoesNotSendHitMessages());

        whenReceiving(aReadRequest()
                .forFile("80D1B8B90CED30430608C58002811B3285FC")
                .withBillingPath("/public/test")
                .withTransferPath("/uploads/50/test")
                .withFileAttributes(attributes().size(10, KiB)
                        .storageInfo(aStorageInfo()))
                .withProtocolInfo(aProtocolInfo().withProtocol("http")
                        .withMajorVersion(1).withIPAddress("192.168.1.1")));

        var reply = replySentWith(endpoint);
        then(reply).should().setFailed(eq(3141), any());
        then(reply).should().setContext(1, null);
        then(billing).shouldHaveNoMoreInteractions();
    }

    @Test
    public void shouldFailReadRequestTriggeringFailedPool2PoolForNonTapeFile() throws Exception
    {
        given(aPartitionManager().withDefault(aPartition().withP2pAllowed(true).withStageAllowed(true)));
        given(aPoolSelectionUnit().withNetUnit("all-net", "192.168.1.1")
                .withProtocolUnit("HTTP", "http/1"));
        given(aPoolMonitor().thatReturns(aPoolSelectorThat()
                .onReadThrows(aPermissionDeniedCacheException())
                .onPool2PoolSelects(aPoolPair()
                        .withSource("source-pool@dCacheDomain")
                        .withDestination("destination-pool@dCacheDomain"))));
        given(aCell("destination-pool@dCacheDomain").communicatingVia(endpoint)
                .thatOnReceiving(Pool2PoolTransferMsg.class).repliesWithReturnCode(3141));
        given(aContainer("PoolManager@dCacheDomain").thatDoesNotSendHitMessages());

        whenReceiving(aReadRequest()
                .forFile("80D1B8B90CED30430608C58002811B3285FC")
                .withBillingPath("/public/test")
                .withTransferPath("/uploads/50/test")
                .withFileAttributes(attributes().size(10, KiB)
                        .storageInfo(aStorageInfo()))
                .withProtocolInfo(aProtocolInfo().withProtocol("http")
                        .withMajorVersion(1).withIPAddress("192.168.1.1")));

        var reply = replySentWith(endpoint);
        then(reply).should().setFailed(eq(3141), any());
        then(reply).should().setContext(1, null);
        then(billing).shouldHaveNoMoreInteractions();
    }

    @Test
    public void shouldFailReadRequestTriggeringFailedPool2PoolWithErrorForNonTapeFile() throws Exception
    {
        given(aPartitionManager().withDefault(aPartition().withP2pAllowed(true).withStageAllowed(true)));
        given(aPoolSelectionUnit().withNetUnit("all-net", "192.168.1.1")
                .withProtocolUnit("HTTP", "http/1"));
        given(aPoolMonitor().thatReturns(aPoolSelectorThat()
                .onReadThrows(aPermissionDeniedCacheException())
                .onPool2PoolSelects(aPoolPair()
                        .withSource("source-pool@dCacheDomain")
                        .withDestination("destination-pool@dCacheDomain"))));
        given(aCell("destination-pool@dCacheDomain").communicatingVia(endpoint)
                .thatOnReceiving(Pool2PoolTransferMsg.class)
                .repliesWithErrorObject("xyzzy").andReturnCode(3141));
        given(aContainer("PoolManager@dCacheDomain").thatDoesNotSendHitMessages());

        whenReceiving(aReadRequest()
                .forFile("80D1B8B90CED30430608C58002811B3285FC")
                .withBillingPath("/public/test")
                .withTransferPath("/uploads/50/test")
                .withFileAttributes(attributes().size(10, KiB)
                        .storageInfo(aStorageInfo()))
                .withProtocolInfo(aProtocolInfo().withProtocol("http")
                        .withMajorVersion(1).withIPAddress("192.168.1.1")));

        var reply = replySentWith(endpoint);
        then(reply).should().setFailed(eq(3141), contains("xyzzy"));
        then(reply).should().setContext(1, null);
        then(billing).shouldHaveNoMoreInteractions();
    }

    @Test
    public void shouldStageOnReadRequestWithFailedPool2PoolForTapeFile() throws Exception
    {
        var stagePool = aPool("stage-pool@dCacheDomain");
        given(aPartitionManager().withDefault(aPartition().withP2pAllowed(true).withStageAllowed(true)));
        given(aPoolSelectionUnit().withNetUnit("all-net", "192.168.1.1")
                .withProtocolUnit("HTTP", "http/1"));
        given(aPoolMonitor().thatReturns(aPoolSelectorThat()
                .onReadThrows(aPermissionDeniedCacheException())
                .onPool2PoolSelects(aPoolPair()
                        .withSource("source-pool@dCacheDomain")
                        .withDestination("destination-pool@dCacheDomain"))
                        .onStageSelects(stagePool)));
        given(aCell("destination-pool@dCacheDomain").communicatingVia(endpoint)
                        .thatOnReceiving(Pool2PoolTransferMsg.class).repliesWithReturnCode(3141)
              .andAnotherCell("stage-pool@dCacheDomain")
                        .thatOnReceiving(PoolFetchFileMessage.class).repliesWithSuccess());
        given(aContainer("PoolManager@dCacheDomain").thatDoesNotSendHitMessages());

        whenReceiving(aReadRequest()
                .by(ROOT)
                .forFile("80D1B8B90CED30430608C58002811B3285FC")
                .withBillingPath("/public/test")
                .withTransferPath("/uploads/50/test")
                .withFileAttributes(attributes().size(10, KiB)
                        .storageInfo(aStorageInfo().withLocation("osm://RZ1/bfid1")))
                .withProtocolInfo(aProtocolInfo().withProtocol("http")
                        .withMajorVersion(1).withIPAddress("192.168.1.1")));

        var reply = replySentWith(endpoint);
        then(reply).should().setSucceeded();
        then(reply).should().setContext(1, stagePool);
        var selectedPool = poolSetInMessage(reply);
        assertThat(selectedPool.getName(), equalTo("stage-pool"));
        assertThat(selectedPool.getAddress(), equalTo(new CellAddressCore("stage-pool@dCacheDomain")));
        then(billing).shouldHaveNoInteractions();
    }

    @Test
    public void shouldFailReplicateRequestWithFailWhileWaitingForP2pResult() throws Exception
    {
        given(aPartitionManager().withDefault(aPartition().withP2pAllowed(true)));
        given(aPoolSelectionUnit().withNetUnit("all-net", "192.168.1.1")
                .withProtocolUnit("HTTP", "http/1"));
        given(aPoolMonitor().thatReturns(aPoolSelectorThat()
                .onPool2PoolSelects(aPoolPair()
                        .withSource("source-pool@dCacheDomain")
                        .withDestination("destination-pool@dCacheDomain"))));
        given(aContainer("PoolManager@dCacheDomain").thatDoesNotSendHitMessages());
        var id = given(aReplicateRequest()
                .forFile("80D1B8B90CED30430608C58002811B3285FC")
                .withPath("/public/test")
                .withFileAttributes(attributes().size(10, KiB).storageInfo(aStorageInfo()))
                .withProtocolInfo(aProtocolInfo().withProtocol("http")
                        .withMajorVersion(1).withIPAddress("192.168.1.1")));

        whenExecutedAdminCommand("rc failed " + id);

        var reply = replySentWith(endpoint);
        then(reply).should().setFailed(eq(1), any());
        then(reply).should().setContext(1, null);
        then(billing).shouldHaveNoInteractions();
    }

    @Test
    public void shouldFailReplicateRequestWithFailWithCodeWhileWaitingForP2pResult() throws Exception
    {
        given(aPartitionManager().withDefault(aPartition().withP2pAllowed(true)));
        given(aPoolSelectionUnit().withNetUnit("all-net", "192.168.1.1")
                .withProtocolUnit("HTTP", "http/1"));
        given(aPoolMonitor().thatReturns(aPoolSelectorThat()
                .onPool2PoolSelects(aPoolPair()
                        .withSource("source-pool@dCacheDomain")
                        .withDestination("destination-pool@dCacheDomain"))));
        given(aContainer("PoolManager@dCacheDomain").thatDoesNotSendHitMessages());
        var id = given(aReplicateRequest()
                .forFile("80D1B8B90CED30430608C58002811B3285FC")
                .withPath("/public/test")
                .withFileAttributes(attributes().size(10, KiB).storageInfo(aStorageInfo()))
                .withProtocolInfo(aProtocolInfo().withProtocol("http")
                        .withMajorVersion(1).withIPAddress("192.168.1.1")));

        whenExecutedAdminCommand("rc failed " + id + " 3141 ");

        var reply = replySentWith(endpoint);
        then(reply).should().setFailed(eq(3141), any());
        then(reply).should().setContext(1, null);
        then(billing).shouldHaveNoInteractions();
    }

    @Test
    public void shouldFailReplicateRequestWithFailWithCodeAndMessageWhileWaitingForP2pResult() throws Exception
    {
        given(aPartitionManager().withDefault(aPartition().withP2pAllowed(true)));
        given(aPoolSelectionUnit().withNetUnit("all-net", "192.168.1.1")
                .withProtocolUnit("HTTP", "http/1"));
        given(aPoolMonitor().thatReturns(aPoolSelectorThat()
                .onPool2PoolSelects(aPoolPair()
                        .withSource("source-pool@dCacheDomain")
                        .withDestination("destination-pool@dCacheDomain"))));
        given(aContainer("PoolManager@dCacheDomain").thatDoesNotSendHitMessages());
        var id = given(aReplicateRequest()
                .forFile("80D1B8B90CED30430608C58002811B3285FC")
                .withPath("/public/test")
                .withFileAttributes(attributes().size(10, KiB).storageInfo(aStorageInfo()))
                .withProtocolInfo(aProtocolInfo().withProtocol("http")
                        .withMajorVersion(1).withIPAddress("192.168.1.1")));

        whenExecutedAdminCommand("rc failed " + id + " 3141 \"It's not going to work!\"");

        var reply = replySentWith(endpoint);
        then(reply).should().setFailed(3141, "It's not going to work!");
        then(reply).should().setContext(1, null);
        then(billing).shouldHaveNoInteractions();
    }

    @Test
    public void shouldRetryReplicateRequestWhileWaitingForP2pResult() throws Exception
    {
        given(aPartitionManager().withDefault(aPartition().withP2pAllowed(true)));
        given(aPoolSelectionUnit().withNetUnit("all-net", "192.168.1.1")
                .withProtocolUnit("HTTP", "http/1"));
        given(aPoolMonitor().thatReturns(aPoolSelectorThat()
                .onPool2PoolSelects(aPoolPair()
                        .withSource("source-pool@dCacheDomain")
                        .withDestination("destination-pool@dCacheDomain"))));
        given(aContainer("PoolManager@dCacheDomain").thatDoesNotSendHitMessages());
        var id = given(aReplicateRequest()
                .forFile("80D1B8B90CED30430608C58002811B3285FC")
                .withPath("/public/test")
                .withFileAttributes(attributes().size(10, KiB).storageInfo(aStorageInfo()))
                .withProtocolInfo(aProtocolInfo().withProtocol("http")
                        .withMajorVersion(1).withIPAddress("192.168.1.1")));

        whenExecutedAdminCommand("rc retry " + id);

        var reply = replySentWith(endpoint);
        then(reply).should().setFailed(eq(10021), any());
        then(reply).should().setContext(0, null);
        then(billing).shouldHaveNoInteractions();
    }

    @Test
    public void shouldTimeoutRequestWhenP2pDestinationPoolRestarted() throws Exception
    {
        given(aPartitionManager().withDefault(aPartition().withP2pAllowed(true)));
        given(aPoolSelectionUnit().withNetUnit("all-net", "192.168.1.1")
                .withProtocolUnit("HTTP", "http/1"));
        given(aPoolMonitor().thatReturns(aPoolSelectorThat()
                .onPool2PoolSelects(aPoolPair()
                        .withSource("source-pool@dCacheDomain")
                        .withDestination("destination-pool@dCacheDomain"))));
        given(aCell("destination-pool@dCacheDomain").communicatingVia(pool).thatOnAdminCommand("pp ls").repliesWith(""));
        given(aContainer("PoolManager@dCacheDomain").thatDoesNotSendHitMessages());
        given(aReplicateRequest()
                .forFile("80D1B8B90CED30430608C58002811B3285FC")
                .withPath("/public/test")
                .withFileAttributes(attributes().size(10, KiB).storageInfo(aStorageInfo()))
                .withProtocolInfo(aProtocolInfo().withProtocol("http")
                        .withMajorVersion(1).withIPAddress("192.168.1.1")));

        whenPingSend();

        var reply = replySentWith(endpoint);
        then(reply).should().setFailed(eq(10006), any());
        then(reply).should().setContext(1, null);
        then(billing).shouldHaveNoInteractions();
    }

    @Test
    public void shouldFailRequestWhenPoolReturnsUnknownMessage() throws Exception
    {
        given(aPartitionManager().withDefault(aPartition().withP2pAllowed(true)));
        given(aPoolSelectionUnit().withNetUnit("all-net", "192.168.1.1")
                .withProtocolUnit("HTTP", "http/1"));
        given(aPoolMonitor().thatReturns(aPoolSelectorThat()
                .onPool2PoolSelects(aPoolPair()
                        .withSource("source-pool@dCacheDomain")
                        .withDestination("destination-pool@dCacheDomain"))));
        given(aCell("destination-pool@dCacheDomain").communicatingVia(endpoint)
            .thatOnReceiving(Pool2PoolTransferMsg.class).repliesWith(PoolFetchFileMessage.class));
        given(aContainer("PoolManager@dCacheDomain").thatDoesNotSendHitMessages());

        whenReceiving(aReplicateRequest()
                .forFile("80D1B8B90CED30430608C58002811B3285FC")
                .withPath("/public/test")
                .withFileAttributes(attributes().size(10, KiB).storageInfo(aStorageInfo()))
                .withProtocolInfo(aProtocolInfo().withProtocol("http")
                        .withMajorVersion(1).withIPAddress("192.168.1.1")));

        var reply = replySentWith(endpoint);
        then(reply).should().setFailed(eq(102), any());
        then(reply).should().setContext(1, null);
        then(billing).shouldHaveNoInteractions();
    }

    @Test
    public void shouldSuspendTapeOnlyFileWhenSuspendOn() throws Exception
    {
        var storageInfo = aStorageInfo().withLocation("osm://RZ1/bfid1").build();
        given(aPartitionManager().withDefault(aPartition().withStageAllowed(true)));
        given(aPoolSelectionUnit().withNetUnit("all-net", "192.168.1.1")
                .withProtocolUnit("HTTP", "http/1"));
        given(aPoolMonitor().thatReturns(aPoolSelectorThat()
                .onReadThrows(aFileNotInCacheException())
                .onStageSelects("stage-pool@dCacheDomain")));
        given(aCell("stage-pool@dCacheDomain").communicatingVia(endpoint)
            .thatOnReceiving(PoolFetchFileMessage.class).repliesWithSuccess());
        given(aContainer("PoolManager@dCacheDomain")
                .thatDoesNotSendHitMessages()
                .withConfig("rc suspend on"));

        whenReceiving(aReadRequest()
                .by(ROOT)
                .forFile("80D1B8B90CED30430608C58002811B3285FC")
                .withBillingPath("/public/test")
                .withTransferPath("/uploads/50/test")
                .withFileAttributes(attributes().size(10, KiB).storageInfo(storageInfo))
                .withProtocolInfo(aProtocolInfo().withProtocol("http")
                        .withMajorVersion(1).withIPAddress("192.168.1.1")));

        then(endpoint).shouldHaveNoInteractions();

        var warning = notificationSentWith(billing, WarningPnfsFileInfoMessage.class);
        assertThat(warning.getResultCode(), equalTo(0));
        assertThat(warning.getCellAddress(), equalTo(new CellAddressCore("PoolManager@dCacheDomain")));
        assertThat(warning.getBillingPath(), equalTo("/public/test"));
        assertThat(warning.getTransferPath(), equalTo("/uploads/50/test"));
        assertThat(warning.getFileSize(), equalTo(KiB.toBytes(10L)));
        assertThat(warning.getStorageInfo(), is(storageInfo));

        whenExecutedAdminCommand("rc retry * -force-all");

        var reply = replySentWith(endpoint);
        then(reply).should().setFailed(eq(10021), any());
        then(reply).should().setContext(0, null);
        then(billing).shouldHaveNoMoreInteractions();
    }

    @Test
    public void shouldFailStageWhenMaxRestoreExceeded() throws Exception
    {
        var stagePool = aPool("stage-pool@dCacheDomain");
        var storageInfo = aStorageInfo().withLocation("osm://RZ1/bfid1").build();
        given(aPartitionManager().withDefault(aPartition().withStageAllowed(true)));
        given(aPoolSelectionUnit().withNetUnit("all-net", "192.168.1.1")
                .withProtocolUnit("HTTP", "http/1"));
        given(aPoolMonitor().thatReturns(aPoolSelectorThat()
                .onReadThrows(aFileNotInCacheException())
                .onStageSelects(stagePool)));
        given(aCell("stage-pool@dCacheDomain").communicatingVia(endpoint)
            .thatOnReceiving(PoolFetchFileMessage.class).repliesWithSuccess());
        given(aContainer("PoolManager@dCacheDomain")
                .thatDoesNotSendHitMessages()
                .withConfig("rc set max restore 0"));

        whenReceiving(aReadRequest()
                .by(ROOT)
                .forFile("80D1B8B90CED30430608C58002811B3285FC")
                .withBillingPath("/public/test")
                .withTransferPath("/uploads/50/test")
                .withFileAttributes(attributes().size(10, KiB).storageInfo(storageInfo))
                .withProtocolInfo(aProtocolInfo().withProtocol("http")
                        .withMajorVersion(1).withIPAddress("192.168.1.1")));

        var warning = notificationSentWith(billing, WarningPnfsFileInfoMessage.class);
        assertThat(warning.getResultCode(), equalTo(5));
        assertThat(warning.getCellAddress(), equalTo(new CellAddressCore("PoolManager@dCacheDomain")));
        assertThat(warning.getBillingPath(), equalTo("/public/test"));
        assertThat(warning.getTransferPath(), equalTo("/uploads/50/test"));
        assertThat(warning.getFileSize(), equalTo(KiB.toBytes(10L)));
        assertThat(warning.getStorageInfo(), is(storageInfo));

        var reply = replySentWith(endpoint);
        then(reply).should().setFailed(eq(5), any());
        then(reply).should().setContext(1, stagePool);
    }

    @Test
    public void shouldStageTapeOnlyFileWhenStagePoolSelectionThrowsCostExceptionWithPool() throws Exception
    {
        given(aPartitionManager().withDefault(aPartition().withStageAllowed(true)));
        given(aPoolSelectionUnit().withNetUnit("all-net", "192.168.1.1")
                .withProtocolUnit("HTTP", "http/1"));
        given(aPoolMonitor().thatReturns(aPoolSelectorThat()
                .onReadThrows(aFileNotInCacheException())
                .onStageThrows(aCostException().withPool("stage-pool@dCacheDomain"))));
        given(aCell("stage-pool@dCacheDomain").communicatingVia(endpoint)
            .thatOnReceiving(PoolFetchFileMessage.class).repliesWithSuccess());
        given(aContainer("PoolManager@dCacheDomain").thatDoesNotSendHitMessages());

        whenReceiving(aReadRequest()
                .by(ROOT)
                .forFile("80D1B8B90CED30430608C58002811B3285FC")
                .withBillingPath("/public/test")
                .withTransferPath("/uploads/50/test")
                .withFileAttributes(attributes().size(10, KiB)
                        .storageInfo(aStorageInfo().withLocation("osm://RZ1/bfid1")))
                .withProtocolInfo(aProtocolInfo().withProtocol("http")
                        .withMajorVersion(1).withIPAddress("192.168.1.1")));

        /*
         * Note: dCache currently does NOT send a message to the selected
         * stage pool if the CostException contains a pool.  Therefore, the job
         * remains in ST_WAITING_FOR_STAGING indefinitely.
         *
         * For this reason, the following assertions do not currently pass.
         */

        /*
        var reply = replySendWith(endpoint);
        then(reply).should().setSucceeded();
        then(reply).should().setContext(0, null);
        then(billing).shouldHaveNoMoreInteractions();
        */
    }

    @Test
    public void shouldFailTapeOnlyFileWhenStagePoolSelectionThrowsCostExceptionWithoutPool() throws Exception
    {
        given(aPartitionManager().withDefault(aPartition().withStageAllowed(true)));
        given(aPoolSelectionUnit().withNetUnit("all-net", "192.168.1.1")
                .withProtocolUnit("HTTP", "http/1"));
        given(aPoolMonitor().thatReturns(aPoolSelectorThat()
                .onReadThrows(aFileNotInCacheException())
                .onStageThrows(aCostException())));
        given(aContainer("PoolManager@dCacheDomain")
                .thatDoesNotSendHitMessages()
                .withConfig("rc onerror fail"));

        whenReceiving(aReadRequest()
                .by(ROOT)
                .forFile("80D1B8B90CED30430608C58002811B3285FC")
                .withBillingPath("/public/test")
                .withTransferPath("/uploads/50/test")
                .withFileAttributes(attributes().size(10, KiB)
                        .storageInfo(aStorageInfo().withLocation("osm://RZ1/bfid1")))
                .withProtocolInfo(aProtocolInfo().withProtocol("http")
                        .withMajorVersion(1).withIPAddress("192.168.1.1")));

        var reply = replySentWith(endpoint);
        then(reply).should().setFailed(eq(125), any());
        then(reply).should().setContext(1, null);
        then(billing).shouldHaveNoInteractions();
    }

    @Test
    public void shouldFailTapeOnlyFileWhenStagePoolSelectionThrowsCacheException() throws Exception
    {
        given(aPartitionManager().withDefault(aPartition().withStageAllowed(true)));
        given(aPoolSelectionUnit().withNetUnit("all-net", "192.168.1.1")
                .withProtocolUnit("HTTP", "http/1"));
        given(aPoolMonitor().thatReturns(aPoolSelectorThat()
                .onReadThrows(aFileNotInCacheException())
                .onStageThrows(aCacheException(3141))));
        given(aContainer("PoolManager@dCacheDomain")
                .thatDoesNotSendHitMessages()
                .withConfig("rc onerror fail"));

        whenReceiving(aReadRequest()
                .by(ROOT)
                .forFile("80D1B8B90CED30430608C58002811B3285FC")
                .withBillingPath("/public/test")
                .withTransferPath("/uploads/50/test")
                .withFileAttributes(attributes().size(10, KiB)
                        .storageInfo(aStorageInfo().withLocation("osm://RZ1/bfid1")))
                .withProtocolInfo(aProtocolInfo().withProtocol("http")
                        .withMajorVersion(1).withIPAddress("192.168.1.1")));

        var reply = replySentWith(endpoint);
        then(reply).should().setFailed(eq(3141), any());
        then(reply).should().setContext(1, null);
        then(billing).shouldHaveNoInteractions();
    }

    @Test
    public void shouldFailTapeOnlyFileWhenStagePoolSelectionThrowsIllegalArgumentException() throws Exception
    {
        given(aPartitionManager().withDefault(aPartition().withStageAllowed(true)));
        given(aPoolSelectionUnit().withNetUnit("all-net", "192.168.1.1")
                .withProtocolUnit("HTTP", "http/1"));
        given(aPoolMonitor().thatReturns(aPoolSelectorThat()
                .onReadThrows(aFileNotInCacheException())
                .onStageThrows(anIllegalArgumentException())));
        given(aContainer("PoolManager@dCacheDomain")
                .thatDoesNotSendHitMessages()
                .withConfig("rc onerror fail"));

        whenReceiving(aReadRequest()
                .by(ROOT)
                .forFile("80D1B8B90CED30430608C58002811B3285FC")
                .withBillingPath("/public/test")
                .withTransferPath("/uploads/50/test")
                .withFileAttributes(attributes().size(10, KiB)
                        .storageInfo(aStorageInfo().withLocation("osm://RZ1/bfid1")))
                .withProtocolInfo(aProtocolInfo().withProtocol("http")
                        .withMajorVersion(1).withIPAddress("192.168.1.1")));

        var reply = replySentWith(endpoint);
        then(reply).should().setFailed(eq(128), any());
        then(reply).should().setContext(1, null);
        then(billing).shouldHaveNoInteractions();
    }

    @Test
    public void shouldFailTapeOnlyFileWhenStagePoolSelectionThrowsRuntimeException() throws Exception
    {
        given(aPartitionManager().withDefault(aPartition().withStageAllowed(true)));
        given(aPoolSelectionUnit().withNetUnit("all-net", "192.168.1.1")
                .withProtocolUnit("HTTP", "http/1"));
        given(aPoolMonitor().thatReturns(aPoolSelectorThat()
                .onReadThrows(aFileNotInCacheException())
                .onStageThrows(aRuntimeException())));
        given(aContainer("PoolManager@dCacheDomain")
                .thatDoesNotSendHitMessages()
                .withConfig("rc onerror fail"));

        whenReceiving(aReadRequest()
                .by(ROOT)
                .forFile("80D1B8B90CED30430608C58002811B3285FC")
                .withBillingPath("/public/test")
                .withTransferPath("/uploads/50/test")
                .withFileAttributes(attributes().size(10, KiB)
                        .storageInfo(aStorageInfo().withLocation("osm://RZ1/bfid1")))
                .withProtocolInfo(aProtocolInfo().withProtocol("http")
                        .withMajorVersion(1).withIPAddress("192.168.1.1")));

        var reply = replySentWith(endpoint);
        // With dCache v7.1, bugs in PoolSelector no longer handled specially.
        // Therefore, there door receives a "generic bug" return code (10011)
        // instead of a PoolSelector-specific bug return code (128).
        then(reply).should().setFailed(eq(10011), any());
        then(reply).should().setContext(1, null);
        then(billing).shouldHaveNoInteractions();
    }

    @Test
    public void shouldReplyOutOfDateForReadRequestForTapeFileWhenStagingWithP2pForTransfer() throws Exception
    {
        var stagePool = aPool("stage-pool@dCacheDomain");
        given(aPartitionManager().withDefault(aPartition().withStageAllowed(true).withP2pForTransfer(true)));
        given(aPoolSelectionUnit().withNetUnit("all-net", "192.168.1.1")
                .withProtocolUnit("HTTP", "http/1"));
        given(aPoolMonitor().thatReturns(aPoolSelectorThat()
                .onReadThrows(aFileNotInCacheException())
                .onStageSelects(stagePool)));
        given(aCell("stage-pool@dCacheDomain").communicatingVia(endpoint)
            .thatOnReceiving(PoolFetchFileMessage.class).repliesWithSuccess());
        given(aContainer("PoolManager@dCacheDomain").thatDoesNotSendHitMessages());

        whenReceiving(aReadRequest()
                .by(ROOT)
                .forFile("80D1B8B90CED30430608C58002811B3285FC")
                .withBillingPath("/public/test")
                .withTransferPath("/uploads/50/test")
                .withFileAttributes(attributes().size(10, KiB)
                        .storageInfo(aStorageInfo().withLocation("osm://RZ1/bfid1")))
                .withProtocolInfo(aProtocolInfo().withProtocol("http")
                        .withMajorVersion(1).withIPAddress("192.168.1.1")));

        var reply = replySentWith(endpoint);
        then(reply).should().setFailed(eq(10021), any());
        then(reply).should().setContext(1, stagePool);
        then(billing).shouldHaveNoInteractions();
    }

    @Test
    public void shouldSuspendReadRequestForTapeFileWhenStagePoolRequestsDelay() throws Exception
    {
        var storageInfo = aStorageInfo().withLocation("osm://RZ1/bfid1").build();
        var stagePool = aPool("stage-pool@dCacheDomain");
        given(aPartitionManager().withDefault(aPartition().withStageAllowed(true).withP2pForTransfer(true)));
        given(aPoolSelectionUnit().withNetUnit("all-net", "192.168.1.1")
                .withProtocolUnit("HTTP", "http/1"));
        given(aPoolMonitor().thatReturns(aPoolSelectorThat()
                .onReadThrows(aFileNotInCacheException())
                .onStageSelects(stagePool)));
        given(aCell("stage-pool@dCacheDomain").communicatingVia(endpoint)
            .thatOnReceiving(PoolFetchFileMessage.class).repliesWithReturnCode(10013));
        given(aContainer("PoolManager@dCacheDomain").thatDoesNotSendHitMessages());

        whenReceiving(aReadRequest()
                .by(ROOT)
                .forFile("80D1B8B90CED30430608C58002811B3285FC")
                .withBillingPath("/public/test")
                .withTransferPath("/uploads/50/test")
                .withFileAttributes(attributes().size(10, KiB).storageInfo(storageInfo))
                .withProtocolInfo(aProtocolInfo().withProtocol("http")
                        .withMajorVersion(1).withIPAddress("192.168.1.1")));

        var warning = notificationSentWith(billing, WarningPnfsFileInfoMessage.class);
        assertThat(warning.getResultCode(), equalTo(0));
        assertThat(warning.getCellAddress(), equalTo(new CellAddressCore("PoolManager@dCacheDomain")));
        assertThat(warning.getBillingPath(), equalTo("/public/test"));
        assertThat(warning.getTransferPath(), equalTo("/uploads/50/test"));
        assertThat(warning.getFileSize(), equalTo(KiB.toBytes(10L)));
        assertThat(warning.getStorageInfo(), is(storageInfo));

        whenExecutedAdminCommand("rc retry * -force-all");

        var reply = replySentWith(endpoint);
        then(reply).should().setFailed(eq(10021), any());
        then(reply).should().setContext(0, stagePool);
        then(billing).shouldHaveNoMoreInteractions();
    }

    @Test
    public void shouldSuspendReadRequestForTapeFileWhenStagePoolRequestsDelayWithSpecificMessage() throws Exception
    {
        var storageInfo = aStorageInfo().withLocation("osm://RZ1/bfid1").build();
        var stagePool = aPool("stage-pool@dCacheDomain");
        given(aPartitionManager().withDefault(aPartition().withStageAllowed(true).withP2pForTransfer(true)));
        given(aPoolSelectionUnit().withNetUnit("all-net", "192.168.1.1")
                .withProtocolUnit("HTTP", "http/1"));
        given(aPoolMonitor().thatReturns(aPoolSelectorThat()
                .onReadThrows(aFileNotInCacheException())
                .onStageSelects(stagePool)));
        given(aCell("stage-pool@dCacheDomain").communicatingVia(endpoint)
                .thatOnReceiving(PoolFetchFileMessage.class).repliesWithErrorObject("xyzzy").andReturnCode(10013));
        given(aContainer("PoolManager@dCacheDomain").thatDoesNotSendHitMessages());

        whenReceiving(aReadRequest()
                .by(ROOT)
                .forFile("80D1B8B90CED30430608C58002811B3285FC")
                .withBillingPath("/public/test")
                .withTransferPath("/uploads/50/test")
                .withFileAttributes(attributes().size(10, KiB).storageInfo(storageInfo))
                .withProtocolInfo(aProtocolInfo().withProtocol("http")
                        .withMajorVersion(1).withIPAddress("192.168.1.1")));

        var warning = notificationSentWith(billing, WarningPnfsFileInfoMessage.class);
        assertThat(warning.getResultCode(), equalTo(0));
        //assertThat(warning.getMessage(), containsString("xyzzy"));  FIXME: cannot assert due to bug in dCache.
        assertThat(warning.getCellAddress(), equalTo(new CellAddressCore("PoolManager@dCacheDomain")));
        assertThat(warning.getBillingPath(), equalTo("/public/test"));
        assertThat(warning.getTransferPath(), equalTo("/uploads/50/test"));
        assertThat(warning.getFileSize(), equalTo(KiB.toBytes(10L)));
        assertThat(warning.getStorageInfo(), is(storageInfo));

        whenExecutedAdminCommand("rc retry * -force-all");

        var reply = replySentWith(endpoint);
        then(reply).should().setFailed(eq(10021), any());
        then(reply).should().setContext(0, stagePool);
        then(billing).shouldHaveNoMoreInteractions();
    }

    @Test
    public void shouldFailReadRequestForTapeFileWhenStagePoolReturnsError() throws Exception
    {
        var stagePool = aPool("stage-pool@dCacheDomain");
        given(aPartitionManager().withDefault(aPartition().withStageAllowed(true).withP2pForTransfer(true)));
        given(aPoolSelectionUnit().withNetUnit("all-net", "192.168.1.1")
                .withProtocolUnit("HTTP", "http/1"));
        given(aPoolMonitor().thatReturns(aPoolSelectorThat()
                .onReadThrows(aFileNotInCacheException())
                .onStageSelects(stagePool)));
        given(aCell("stage-pool@dCacheDomain").communicatingVia(endpoint)
            .thatOnReceiving(PoolFetchFileMessage.class).repliesWithReturnCode(3141));
        given(aContainer("PoolManager@dCacheDomain").thatDoesNotSendHitMessages());

        whenReceiving(aReadRequest()
                .by(ROOT)
                .forFile("80D1B8B90CED30430608C58002811B3285FC")
                .withBillingPath("/public/test")
                .withTransferPath("/uploads/50/test")
                .withFileAttributes(attributes().size(10, KiB)
                        .storageInfo(aStorageInfo().withLocation("osm://RZ1/bfid1")))
                .withProtocolInfo(aProtocolInfo().withProtocol("http")
                        .withMajorVersion(1).withIPAddress("192.168.1.1")));

        var reply = replySentWith(endpoint);
        then(reply).should().setFailed(eq(3141), any());
        then(reply).should().setContext(1, stagePool);
        then(billing).shouldHaveNoInteractions();
    }

    @Test
    public void shouldFailReadRequestForTapeFileWhenStagePoolReturnsErrorWithMessage() throws Exception
    {
        var stagePool = aPool("stage-pool@dCacheDomain");
        given(aPartitionManager().withDefault(aPartition().withStageAllowed(true).withP2pForTransfer(true)));
        given(aPoolSelectionUnit().withNetUnit("all-net", "192.168.1.1")
                .withProtocolUnit("HTTP", "http/1"));
        given(aPoolMonitor().thatReturns(aPoolSelectorThat()
                .onReadThrows(aFileNotInCacheException())
                .onStageSelects(stagePool)));
        given(aCell("stage-pool@dCacheDomain").communicatingVia(endpoint)
            .thatOnReceiving(PoolFetchFileMessage.class)
                .repliesWithErrorObject("xyzzy").repliesWithReturnCode(3141));
        given(aContainer("PoolManager@dCacheDomain").thatDoesNotSendHitMessages());

        whenReceiving(aReadRequest()
                .by(ROOT)
                .forFile("80D1B8B90CED30430608C58002811B3285FC")
                .withBillingPath("/public/test")
                .withTransferPath("/uploads/50/test")
                .withFileAttributes(attributes().size(10, KiB)
                        .storageInfo(aStorageInfo().withLocation("osm://RZ1/bfid1")))
                .withProtocolInfo(aProtocolInfo().withProtocol("http")
                        .withMajorVersion(1).withIPAddress("192.168.1.1")));

        var reply = replySentWith(endpoint);
        then(reply).should().setFailed(eq(3141), contains("xyzzy"));
        then(reply).should().setContext(1, stagePool);
        then(billing).shouldHaveNoInteractions();
    }

    @Test
    public void shouldFailReadRequestForTapeFileWhileStagingWhenAdminFailsRequest() throws Exception
    {
        var stagePool = aPool("stage-pool@dCacheDomain");
        given(aPartitionManager().withDefault(aPartition().withStageAllowed(true).withP2pForTransfer(true)));
        given(aPoolSelectionUnit().withNetUnit("all-net", "192.168.1.1")
                .withProtocolUnit("HTTP", "http/1"));
        given(aPoolMonitor().thatReturns(aPoolSelectorThat()
                .onReadThrows(aFileNotInCacheException())
                .onStageSelects(stagePool)));
        given(aContainer("PoolManager@dCacheDomain").thatDoesNotSendHitMessages());
        var id = given(aReadRequest()
                .by(ROOT)
                .forFile("80D1B8B90CED30430608C58002811B3285FC")
                .withBillingPath("/public/test")
                .withTransferPath("/uploads/50/test")
                .withFileAttributes(attributes().size(10, KiB)
                        .storageInfo(aStorageInfo().withLocation("osm://RZ1/bfid1")))
                .withProtocolInfo(aProtocolInfo().withProtocol("http")
                        .withMajorVersion(1).withIPAddress("192.168.1.1")));

        whenExecutedAdminCommand("rc failed " + id);

        var reply = replySentWith(endpoint);
        then(reply).should().setFailed(eq(1), any());
        then(reply).should().setContext(1, stagePool);
        then(billing).shouldHaveNoInteractions();
    }

    @Test
    public void shouldFailReadRequestForTapeFileWhileStagingWhenAdminFailsRequestWithRc() throws Exception
    {
        var stagePool = aPool("stage-pool@dCacheDomain");
        given(aPartitionManager().withDefault(aPartition().withStageAllowed(true).withP2pForTransfer(true)));
        given(aPoolSelectionUnit().withNetUnit("all-net", "192.168.1.1")
                .withProtocolUnit("HTTP", "http/1"));
        given(aPoolMonitor().thatReturns(aPoolSelectorThat()
                .onReadThrows(aFileNotInCacheException())
                .onStageSelects(stagePool)));
        given(aContainer("PoolManager@dCacheDomain").thatDoesNotSendHitMessages());
        var id = given(aReadRequest()
                .by(ROOT)
                .forFile("80D1B8B90CED30430608C58002811B3285FC")
                .withBillingPath("/public/test")
                .withTransferPath("/uploads/50/test")
                .withFileAttributes(attributes().size(10, KiB)
                        .storageInfo(aStorageInfo().withLocation("osm://RZ1/bfid1")))
                .withProtocolInfo(aProtocolInfo().withProtocol("http")
                        .withMajorVersion(1).withIPAddress("192.168.1.1")));

        whenExecutedAdminCommand("rc failed " + id + " 3141");

        var reply = replySentWith(endpoint);
        then(reply).should().setFailed(eq(3141), any());
        then(reply).should().setContext(1, stagePool);
        then(billing).shouldHaveNoInteractions();
    }

    @Test
    public void shouldFailReadRequestForTapeFileWhileStagingWhenAdminFailsRequestWithRcAndMessage() throws Exception
    {
        var stagePool = aPool("stage-pool@dCacheDomain");
        given(aPartitionManager().withDefault(aPartition().withStageAllowed(true).withP2pForTransfer(true)));
        given(aPoolSelectionUnit().withNetUnit("all-net", "192.168.1.1")
                .withProtocolUnit("HTTP", "http/1"));
        given(aPoolMonitor().thatReturns(aPoolSelectorThat()
                .onReadThrows(aFileNotInCacheException())
                .onStageSelects(stagePool)));
        given(aContainer("PoolManager@dCacheDomain").thatDoesNotSendHitMessages());
        var id = given(aReadRequest()
                .by(ROOT)
                .forFile("80D1B8B90CED30430608C58002811B3285FC")
                .withBillingPath("/public/test")
                .withTransferPath("/uploads/50/test")
                .withFileAttributes(attributes().size(10, KiB)
                        .storageInfo(aStorageInfo().withLocation("osm://RZ1/bfid1")))
                .withProtocolInfo(aProtocolInfo().withProtocol("http")
                        .withMajorVersion(1).withIPAddress("192.168.1.1")));

        whenExecutedAdminCommand("rc failed " + id + " 3141 xyzzy");

        var reply = replySentWith(endpoint);
        then(reply).should().setFailed(eq(3141), contains("xyzzy"));
        then(reply).should().setContext(1, stagePool);
        then(billing).shouldHaveNoInteractions();
    }

    @Test
    public void shouldTimeoutReadRequestForTapeFileWhenStagingPoolRestarted() throws Exception
    {
        var stagePool = aPool("stage-pool@dCacheDomain");
        given(aPartitionManager().withDefault(aPartition().withStageAllowed(true).withP2pForTransfer(true)));
        given(aPoolSelectionUnit().withNetUnit("all-net", "192.168.1.1")
                .withProtocolUnit("HTTP", "http/1"));
        given(aPoolMonitor().thatReturns(aPoolSelectorThat()
                .onReadThrows(aFileNotInCacheException())
                .onStageSelects(stagePool)));
        given(aCell("stage-pool@dCacheDomain").communicatingVia(pool)
            .thatOnAdminCommand("rh ls").repliesWith(""));
        given(aContainer("PoolManager@dCacheDomain").thatDoesNotSendHitMessages());
        given(aReadRequest()
                .by(ROOT)
                .forFile("80D1B8B90CED30430608C58002811B3285FC")
                .withBillingPath("/public/test")
                .withTransferPath("/uploads/50/test")
                .withFileAttributes(attributes().size(10, KiB)
                        .storageInfo(aStorageInfo().withLocation("osm://RZ1/bfid1")))
                .withProtocolInfo(aProtocolInfo().withProtocol("http")
                        .withMajorVersion(1).withIPAddress("192.168.1.1")));

        whenPingSend();

        var reply = replySentWith(endpoint);
        then(reply).should().setFailed(eq(10006), any());
        then(reply).should().setContext(1, stagePool);
        then(billing).shouldHaveNoInteractions();
    }

    @Test
    public void shouldFailReadRequestForTapeFileWhenStagingAndAdminFailsRequest() throws Exception
    {
        var stagePool = aPool("stage-pool@dCacheDomain");
        given(aPartitionManager().withDefault(aPartition().withStageAllowed(true).withP2pForTransfer(true)));
        given(aPoolSelectionUnit().withNetUnit("all-net", "192.168.1.1")
                .withProtocolUnit("HTTP", "http/1"));
        given(aPoolMonitor().thatReturns(aPoolSelectorThat()
                .onReadThrows(aFileNotInCacheException())
                .onStageSelects(stagePool)));
        given(aContainer("PoolManager@dCacheDomain").thatDoesNotSendHitMessages());
        var id = given(aReadRequest()
                .by(ROOT)
                .forFile("80D1B8B90CED30430608C58002811B3285FC")
                .withBillingPath("/public/test")
                .withTransferPath("/uploads/50/test")
                .withFileAttributes(attributes().size(10, KiB)
                        .storageInfo(aStorageInfo().withLocation("osm://RZ1/bfid1")))
                .withProtocolInfo(aProtocolInfo().withProtocol("http")
                        .withMajorVersion(1).withIPAddress("192.168.1.1")));

        whenExecutedAdminCommand("rc failed " + id);

        var reply = replySentWith(endpoint);
        then(reply).should().setFailed(eq(1), any());
        then(reply).should().setContext(1, stagePool);
        then(billing).shouldHaveNoInteractions();
    }

    @Test
    public void shouldFailReadRequestForTapeFileWhenStagingAndAdminFailsRequestWithRc() throws Exception
    {
        var stagePool = aPool("stage-pool@dCacheDomain");
        given(aPartitionManager().withDefault(aPartition().withStageAllowed(true).withP2pForTransfer(true)));
        given(aPoolSelectionUnit().withNetUnit("all-net", "192.168.1.1")
                .withProtocolUnit("HTTP", "http/1"));
        given(aPoolMonitor().thatReturns(aPoolSelectorThat()
                .onReadThrows(aFileNotInCacheException())
                .onStageSelects(stagePool)));
        given(aContainer("PoolManager@dCacheDomain").thatDoesNotSendHitMessages());
        var id = given(aReadRequest()
                .by(ROOT)
                .forFile("80D1B8B90CED30430608C58002811B3285FC")
                .withBillingPath("/public/test")
                .withTransferPath("/uploads/50/test")
                .withFileAttributes(attributes().size(10, KiB)
                        .storageInfo(aStorageInfo().withLocation("osm://RZ1/bfid1")))
                .withProtocolInfo(aProtocolInfo().withProtocol("http")
                        .withMajorVersion(1).withIPAddress("192.168.1.1")));

        whenExecutedAdminCommand("rc failed " + id + " 3141");

        var reply = replySentWith(endpoint);
        then(reply).should().setFailed(eq(3141), any());
        then(reply).should().setContext(1, stagePool);
        then(billing).shouldHaveNoInteractions();
    }

    @Test
    public void shouldFailReadRequestForTapeFileWhenStagingAndAdminFailsRequestWithRcAndMessage() throws Exception
    {
        var stagePool = aPool("stage-pool@dCacheDomain");
        given(aPartitionManager().withDefault(aPartition().withStageAllowed(true).withP2pForTransfer(true)));
        given(aPoolSelectionUnit().withNetUnit("all-net", "192.168.1.1")
                .withProtocolUnit("HTTP", "http/1"));
        given(aPoolMonitor().thatReturns(aPoolSelectorThat()
                .onReadThrows(aFileNotInCacheException())
                .onStageSelects(stagePool)));
        given(aContainer("PoolManager@dCacheDomain").thatDoesNotSendHitMessages());
        var id = given(aReadRequest()
                .by(ROOT)
                .forFile("80D1B8B90CED30430608C58002811B3285FC")
                .withBillingPath("/public/test")
                .withTransferPath("/uploads/50/test")
                .withFileAttributes(attributes().size(10, KiB)
                        .storageInfo(aStorageInfo().withLocation("osm://RZ1/bfid1")))
                .withProtocolInfo(aProtocolInfo().withProtocol("http")
                        .withMajorVersion(1).withIPAddress("192.168.1.1")));

        whenExecutedAdminCommand("rc failed " + id + " 3141 xyzzy");

        var reply = replySentWith(endpoint);
        then(reply).should().setFailed(eq(3141), contains("xyzzy"));
        then(reply).should().setContext(1, stagePool);
        then(billing).shouldHaveNoInteractions();
    }

    @Test
    public void shouldFailReadRequestForTapeFileWhenStagingAndPoolRepliesWithUnexceptedMessage() throws Exception
    {
        var stagePool = aPool("stage-pool@dCacheDomain");
        given(aPartitionManager().withDefault(aPartition().withStageAllowed(true).withP2pForTransfer(true)));
        given(aPoolSelectionUnit().withNetUnit("all-net", "192.168.1.1")
                .withProtocolUnit("HTTP", "http/1"));
        given(aPoolMonitor().thatReturns(aPoolSelectorThat()
                .onReadThrows(aFileNotInCacheException())
                .onStageSelects(stagePool)));
        given(aCell("stage-pool@dCacheDomain").communicatingVia(endpoint)
            .thatOnReceiving(PoolFetchFileMessage.class)
                .repliesWith(Pool2PoolTransferMsg.class));
        given(aContainer("PoolManager@dCacheDomain").thatDoesNotSendHitMessages());

        whenReceiving(aReadRequest()
                .by(ROOT)
                .forFile("80D1B8B90CED30430608C58002811B3285FC")
                .withBillingPath("/public/test")
                .withTransferPath("/uploads/50/test")
                .withFileAttributes(attributes().size(10, KiB)
                        .storageInfo(aStorageInfo().withLocation("osm://RZ1/bfid1")))
                .withProtocolInfo(aProtocolInfo().withProtocol("http")
                        .withMajorVersion(1).withIPAddress("192.168.1.1")));

        var reply = replySentWith(endpoint);
        then(reply).should().setFailed(eq(102), any());
        then(reply).should().setContext(1, stagePool);
        then(billing).shouldHaveNoInteractions();
    }

    @Test
    public void shouldFailReadRequestForTapeFileWhenStagingNotAllowedByDoor() throws Exception
    {
        var storageInfo = aStorageInfo().withLocation("osm://RZ1/bfid1").build();
        given(aPartitionManager().withDefault(aPartition().withStageAllowed(true).withP2pForTransfer(true)));
        given(aPoolSelectionUnit().withNetUnit("all-net", "192.168.1.1")
                .withProtocolUnit("HTTP", "http/1"));
        given(aPoolMonitor().thatReturns(aPoolSelectorThat()
                .onReadThrows(aFileNotInCacheException())));
        given(aContainer("PoolManager@dCacheDomain").thatDoesNotSendHitMessages());

        whenReceiving(aReadRequest()
                .withStaging(false)
                .by(ROOT)
                .forFile("80D1B8B90CED30430608C58002811B3285FC")
                .withBillingPath("/public/test")
                .withTransferPath("/uploads/50/test")
                .withFileAttributes(attributes().size(10, KiB).storageInfo(storageInfo))
                .withProtocolInfo(aProtocolInfo().withProtocol("http")
                        .withMajorVersion(1).withIPAddress("192.168.1.1")));

        var warning = notificationSentWith(billing, WarningPnfsFileInfoMessage.class);
        assertThat(warning.getResultCode(), equalTo(10018));
        assertThat(warning.getCellAddress(), equalTo(new CellAddressCore("PoolManager@dCacheDomain")));
        assertThat(warning.getBillingPath(), equalTo("/public/test"));
        assertThat(warning.getTransferPath(), equalTo("/uploads/50/test"));
        assertThat(warning.getFileSize(), equalTo(KiB.toBytes(10L)));
        assertThat(warning.getStorageInfo(), is(storageInfo));


        var reply = replySentWith(endpoint);
        then(reply).should().setFailed(eq(10018), any());
        then(reply).should().setContext(1, null);
    }

    @Test
    public void shouldFailReadRequestForTapeFileWhenStagingNotAllowedForAnonymousUsers() throws Exception
    {
        var storageInfo = aStorageInfo().withLocation("osm://RZ1/bfid1").build();
        given(aPartitionManager().withDefault(aPartition().withStageAllowed(true).withP2pForTransfer(true)));
        given(aPoolSelectionUnit().withNetUnit("all-net", "192.168.1.1")
                .withProtocolUnit("HTTP", "http/1"));
        given(aPoolMonitor().thatReturns(aPoolSelectorThat()
                .onReadThrows(aFileNotInCacheException())));
        given(aContainer("PoolManager@dCacheDomain").thatDoesNotSendHitMessages());

        whenReceiving(aReadRequest()
                .withStaging(true)
                .by(NOBODY)
                .forFile("80D1B8B90CED30430608C58002811B3285FC")
                .withBillingPath("/public/test")
                .withTransferPath("/uploads/50/test")
                .withFileAttributes(attributes().size(10, KiB).storageInfo(storageInfo))
                .withProtocolInfo(aProtocolInfo().withProtocol("http")
                        .withMajorVersion(1).withIPAddress("192.168.1.1")));

        var warning = notificationSentWith(billing, WarningPnfsFileInfoMessage.class);
        assertThat(warning.getResultCode(), equalTo(10018));
        assertThat(warning.getCellAddress(), equalTo(new CellAddressCore("PoolManager@dCacheDomain")));
        assertThat(warning.getBillingPath(), equalTo("/public/test"));
        assertThat(warning.getTransferPath(), equalTo("/uploads/50/test"));
        assertThat(warning.getFileSize(), equalTo(KiB.toBytes(10L)));
        assertThat(warning.getStorageInfo(), is(storageInfo));


        var reply = replySentWith(endpoint);
        then(reply).should().setFailed(eq(10018), any());
        then(reply).should().setContext(1, null);
    }

    @Test
    public void shouldFailReadRequestForFileWhenP2pNotAllowedByDoor() throws Exception
    {
        var storageInfo = aStorageInfo().build();
        given(aPartitionManager().withDefault(aPartition()));
        given(aPoolSelectionUnit().withNetUnit("all-net", "192.168.1.1")
                .withProtocolUnit("HTTP", "http/1"));
        given(aPoolMonitor().thatReturns(aPoolSelectorThat()
                .onReadThrows(aCostException()
                        .withPool("hot-pool@dCacheDomain")
                        .withTryAlternatives(true))
                .returnsCurrentPartition(aPartition().withP2pOnCost(true))));
        given(aContainer("PoolManager@dCacheDomain").thatDoesNotSendHitMessages());

        whenReceiving(aReadRequest()
                .withPool2Pool(false)
                .forFile("80D1B8B90CED30430608C58002811B3285FC")
                .withBillingPath("/public/test")
                .withTransferPath("/uploads/50/test")
                .withFileAttributes(attributes().size(10, KiB).storageInfo(storageInfo))
                .withProtocolInfo(aProtocolInfo().withProtocol("http")
                        .withMajorVersion(1).withIPAddress("192.168.1.1")));

        var warning = notificationSentWith(billing, WarningPnfsFileInfoMessage.class);
        assertThat(warning.getResultCode(), equalTo(10018));
        assertThat(warning.getCellAddress(), equalTo(new CellAddressCore("PoolManager@dCacheDomain")));
        assertThat(warning.getBillingPath(), equalTo("/public/test"));
        assertThat(warning.getTransferPath(), equalTo("/uploads/50/test"));
        assertThat(warning.getFileSize(), equalTo(KiB.toBytes(10L)));
        assertThat(warning.getStorageInfo(), is(storageInfo));


        var reply = replySentWith(endpoint);
        then(reply).should().setFailed(eq(10018), any());
        then(reply).should().setContext(1, null);
    }

    @Test
    public void shouldReplyMultipleRequestsWhenBelowClumpingLimit() throws Exception
    {
        var poolStageRequest = SettableFuture.<CellMessage>create();
        var stagePool = aPool("stage-pool@dCacheDomain");
        given(aPartitionManager().withDefault(aPartition().withStageAllowed(true)));
        given(aPoolSelectionUnit().withNetUnit("all-net", "192.168.1.1")
                .withProtocolUnit("HTTP", "http/1"));
        given(aPoolMonitor().thatReturns(aPoolSelectorThat()
                .onReadThrows(aFileNotInCacheException())
                .onStageSelects(stagePool)));
        given(aContainer("PoolManager@dCacheDomain").thatDoesNotSendHitMessages());
        given(aCell("stage-pool@dCacheDomain").communicatingVia(endpoint)
            .thatOnReceiving(PoolFetchFileMessage.class).storesRequestIn(poolStageRequest));
        given(repeat(20), i -> aReadRequestFrom("door-" + i + "@dCacheDomain")
                .by(ROOT)
                .forFile("80D1B8B90CED30430608C58002811B3285FC")
                .withBillingPath("/public/test")
                .withTransferPath("/uploads/50/test")
                .withFileAttributes(attributes().size(10, KiB)
                        .storageInfo(aStorageInfo().withLocation("osm://RZ1/bfid1")))
                .withProtocolInfo(aProtocolInfo().withProtocol("http")
                        .withMajorVersion(1).withIPAddress("192.168.1.1")));

        whenReceiving(aResponseTo(poolStageRequest).withRc(0));

        List<PoolMgrSelectReadPoolMsg> allReplies = allRepliesSentWith(endpoint);
        assertThat(allReplies.size(), equalTo(20));
        for (PoolMgrSelectReadPoolMsg reply : allReplies) {
            then(reply).should().setSucceeded();
            then(reply).should().setContext(1, stagePool);
        }
        then(billing).shouldHaveNoInteractions();
    }

    @Test
    public void shouldReturnRetryForRequestsBeyondClumpLimit() throws Exception
    {
        var poolStageRequest = SettableFuture.<CellMessage>create();
        var stagePool = aPool("stage-pool@dCacheDomain");
        given(aPartitionManager().withDefault(aPartition().withStageAllowed(true)));
        given(aPoolSelectionUnit().withNetUnit("all-net", "192.168.1.1")
                .withProtocolUnit("HTTP", "http/1"));
        given(aPoolMonitor().thatReturns(aPoolSelectorThat()
                .onReadThrows(aFileNotInCacheException())
                .onStageSelects(stagePool)));
        given(aContainer("PoolManager@dCacheDomain").thatDoesNotSendHitMessages());
        given(aCell("stage-pool@dCacheDomain").communicatingVia(endpoint)
            .thatOnReceiving(PoolFetchFileMessage.class).storesRequestIn(poolStageRequest));
        given(repeat(30), i -> aReadRequestFrom("door-" + i + "@dCacheDomain")
                .by(ROOT)
                .forFile("80D1B8B90CED30430608C58002811B3285FC")
                .withBillingPath("/public/test")
                .withTransferPath("/uploads/50/test")
                .withFileAttributes(attributes().size(10, KiB)
                        .storageInfo(aStorageInfo().withLocation("osm://RZ1/bfid1")))
                .withProtocolInfo(aProtocolInfo().withProtocol("http")
                        .withMajorVersion(1).withIPAddress("192.168.1.1")));

        whenReceiving(aResponseTo(poolStageRequest).withRc(0));

        var allReplies = allRepliesSentWith(endpoint);
        assertThat(allReplies.size(), equalTo(30));
        allReplies.forEach(r -> then(r).should().setContext(1, stagePool));
        allReplies.stream().limit(20).forEach(r -> then(r).should().setSucceeded());
        allReplies.stream().skip(20).forEach(r -> then(r).should().setFailed(eq(10021), any()));
        then(billing).shouldHaveNoInteractions();
    }

    @Test
    public void shouldReturnEmptyInfoListWhenIdle() throws Exception
    {
        var stagePool = aPool("stage-pool@dCacheDomain");
        given(aPartitionManager().withDefault(aPartition().withStageAllowed(true).withP2pForTransfer(true)));
        given(aPoolSelectionUnit().withNetUnit("all-net", "192.168.1.1")
                .withProtocolUnit("HTTP", "http/1"));
        given(aPoolMonitor().thatReturns(aPoolSelectorThat()
                .onReadThrows(aFileNotInCacheException())
                .onStageSelects(stagePool)));
        given(aContainer("PoolManager@dCacheDomain").thatDoesNotSendHitMessages());

        whenReceiving(anInfoRequest());

        var allInfos = allRestoreHandlerInfo(infoResponse);
        assertThat(allInfos, hasSize(0));
    }

    @Test
    public void shouldReturnSingleInfoForSingleStagingFile() throws Exception
    {
        var stagePool = aPool("stage-pool@dCacheDomain");
        given(aPartitionManager().withDefault(aPartition().withStageAllowed(true).withP2pForTransfer(true)));
        given(aPoolSelectionUnit().withNetUnit("all-net", "192.168.1.1")
                .withProtocolUnit("HTTP", "http/1"));
        given(aPoolMonitor().thatReturns(aPoolSelectorThat()
                .onReadThrows(aFileNotInCacheException())
                .onStageSelects(stagePool)));
        given(aContainer("PoolManager@dCacheDomain").thatDoesNotSendHitMessages());

        long startTimeLowerBound = System.currentTimeMillis();
        var id = given(aReadRequest()
                .by(ROOT)
                .forFile("80D1B8B90CED30430608C58002811B3285FC")
                .withBillingPath("/public/test")
                .withTransferPath("/uploads/50/test")
                .withFileAttributes(attributes().size(10, KiB)
                        .storageInfo(aStorageInfo().withLocation("osm://RZ1/bfid1")))
                .withProtocolInfo(aProtocolInfo().withProtocol("http")
                        .withMajorVersion(1).withIPAddress("192.168.1.1")));
        long startTimeUpperBound = System.currentTimeMillis();

        whenReceiving(anInfoRequest());

        var allInfos = allRestoreHandlerInfo(infoResponse);
        assertThat(allInfos, hasSize(1));

        RestoreHandlerInfo info = allInfos.get(0);
        assertThat(info.getClientCount(), is(1));
        assertThat(info.getErrorCode(), is(0));
        assertThat(info.getErrorMessage(), emptyString());
        assertThat(info.getName(), equalTo(id));
        assertThat(info.getPool(), equalTo("stage-pool"));
        assertThat(info.getRetryCount(), equalTo(0));
        assertThat(info.getStartTime(), greaterThanOrEqualTo(startTimeLowerBound));
        assertThat(info.getStartTime(), lessThanOrEqualTo(startTimeUpperBound));
        assertThat(info.getStatus(), not(emptyString()));
    }

    @Test
    public void shouldReturnSingleInfoForReplicatingFile() throws Exception
    {
        given(aPartitionManager().withDefault(aPartition()));
        given(aPoolSelectionUnit().withNetUnit("all-net", "192.168.1.1")
                .withProtocolUnit("HTTP", "http/1"));
        given(aPoolMonitor().thatReturns(aPoolSelectorThat()
                .onPool2PoolSelects(aPoolPair()
                        .withSource("source-pool@dCacheDomain")
                        .withDestination("destination-pool@dCacheDomain"))));
        given(aContainer("PoolManager@dCacheDomain").thatDoesNotSendHitMessages());
        long startTimeLowerBound = System.currentTimeMillis();
        var id = given(aReplicateRequest()
                .forFile("80D1B8B90CED30430608C58002811B3285FC")
                .withPath("/public/test")
                .withFileAttributes(attributes().size(10, KiB).storageInfo(aStorageInfo()))
                .withProtocolInfo(aProtocolInfo().withProtocol("http")
                        .withMajorVersion(1).withIPAddress("192.168.1.1")));
        long startTimeUpperBound = System.currentTimeMillis();

        whenReceiving(anInfoRequest());

        var allInfos = allRestoreHandlerInfo(infoResponse);
        assertThat(allInfos, hasSize(1));

        RestoreHandlerInfo info = allInfos.get(0);
        assertThat(info.getClientCount(), is(1));
        assertThat(info.getErrorCode(), is(0));
        assertThat(info.getErrorMessage(), emptyString());
        assertThat(info.getName(), equalTo(id));
        assertThat(info.getPool(), equalTo("source-pool->destination-pool"));
        assertThat(info.getRetryCount(), equalTo(0));
        assertThat(info.getStartTime(), greaterThanOrEqualTo(startTimeLowerBound));
        assertThat(info.getStartTime(), lessThanOrEqualTo(startTimeUpperBound));
        assertThat(info.getStatus(), not(emptyString()));
    }

    @Test
    public void shouldReturnSingleInfoForSuspendedRequest() throws Exception
    {
        given(aPartitionManager().withDefault(aPartition()));
        given(aPoolSelectionUnit().withNetUnit("all-net", "192.168.1.1")
                .withProtocolUnit("HTTP", "http/1"));
        given(aPoolMonitor().thatReturns(aPoolSelectorThat()));
        given(aContainer("PoolManager@dCacheDomain")
                .thatDoesNotSendHitMessages()
                .withConfig("rc suspend on -all"));
        long startTimeLowerBound = System.currentTimeMillis();
        var id = given(aReadRequest()
                .forFile("80D1B8B90CED30430608C58002811B3285FC")
                .withPath("/public/test")
                .withFileAttributes(attributes().size(10, KiB).storageInfo(aStorageInfo()))
                .withProtocolInfo(aProtocolInfo().withProtocol("http")
                        .withMajorVersion(1).withIPAddress("192.168.1.1")));
        long startTimeUpperBound = System.currentTimeMillis();

        whenReceiving(anInfoRequest());

        var allInfos = allRestoreHandlerInfo(infoResponse);
        assertThat(allInfos, hasSize(1));

        RestoreHandlerInfo info = allInfos.get(0);
        assertThat(info.getClientCount(), is(1));
        assertThat(info.getErrorCode(), is(0));
        assertThat(info.getErrorMessage(), emptyString());
        assertThat(info.getName(), equalTo(id));
        assertThat(info.getPool(), equalTo("<unknown>"));
        assertThat(info.getRetryCount(), equalTo(0));
        assertThat(info.getStartTime(), greaterThanOrEqualTo(startTimeLowerBound));
        assertThat(info.getStartTime(), lessThanOrEqualTo(startTimeUpperBound));
        assertThat(info.getStatus(), not(emptyString()));
    }

    @Test
    public void shouldRetryPool2PoolOnDestinationPoolUp() throws Exception
    {
        given(aPartitionManager().withDefault(aPartition()));
        given(aPoolSelectionUnit().withNetUnit("all-net", "192.168.1.1")
                .withProtocolUnit("HTTP", "http/1"));
        given(aPoolMonitor().thatReturns(aPoolSelectorThat()
                .onPool2PoolSelects(aPoolPair()
                        .withSource("source-pool@dCacheDomain")
                        .withDestination("destination-pool@dCacheDomain"))));
        given(aContainer("PoolManager@dCacheDomain").thatDoesNotSendHitMessages());
        given(aReplicateRequest()
                .forFile("80D1B8B90CED30430608C58002811B3285FC")
                .withPath("/public/test")
                .withFileAttributes(attributes().size(10, KiB).storageInfo(aStorageInfo()))
                .withProtocolInfo(aProtocolInfo().withProtocol("http")
                        .withMajorVersion(1).withIPAddress("192.168.1.1")));

        whenReceiving(aPoolStatusChange().thatPool("destination-pool").isUp());

        var reply = replySentWith(endpoint);
        then(reply).should().setFailed(eq(10021), any());
        then(reply).should().setContext(0, null);
        then(billing).shouldHaveNoInteractions();
    }

    @Test
    public void shouldDoNothingOnPoolUpUnrelatedToOngoingPool2Pool() throws Exception
    {
        given(aPartitionManager().withDefault(aPartition()));
        given(aPoolSelectionUnit().withNetUnit("all-net", "192.168.1.1")
                .withProtocolUnit("HTTP", "http/1"));
        given(aPoolMonitor().thatReturns(aPoolSelectorThat()
                .onPool2PoolSelects(aPoolPair()
                        .withSource("source-pool@dCacheDomain")
                        .withDestination("destination-pool@dCacheDomain"))));
        given(aContainer("PoolManager@dCacheDomain").thatDoesNotSendHitMessages());
        given(aReplicateRequest()
                .forFile("80D1B8B90CED30430608C58002811B3285FC")
                .withPath("/public/test")
                .withFileAttributes(attributes().size(10, KiB).storageInfo(aStorageInfo()))
                .withProtocolInfo(aProtocolInfo().withProtocol("http")
                        .withMajorVersion(1).withIPAddress("192.168.1.1")));
        verify(endpoint).sendMessage(any());

        whenReceiving(aPoolStatusChange().thatPool("unrelated-pool").isUp());

        then(endpoint).shouldHaveNoMoreInteractions();
        then(billing).shouldHaveNoInteractions();
    }

    @Test
    public void shouldRetryPool2PoolOnDestinationPoolDown() throws Exception
    {
        given(aPartitionManager().withDefault(aPartition()));
        given(aPoolSelectionUnit().withNetUnit("all-net", "192.168.1.1")
                .withProtocolUnit("HTTP", "http/1"));
        given(aPoolMonitor().thatReturns(aPoolSelectorThat()
                .onPool2PoolSelects(aPoolPair()
                        .withSource("source-pool@dCacheDomain")
                        .withDestination("destination-pool@dCacheDomain"))));
        given(aContainer("PoolManager@dCacheDomain").thatDoesNotSendHitMessages());
        given(aReplicateRequest()
                .forFile("80D1B8B90CED30430608C58002811B3285FC")
                .withPath("/public/test")
                .withFileAttributes(attributes().size(10, KiB).storageInfo(aStorageInfo()))
                .withProtocolInfo(aProtocolInfo().withProtocol("http")
                        .withMajorVersion(1).withIPAddress("192.168.1.1")));

        whenReceiving(aPoolStatusChange().thatPool("destination-pool").isDown());

        var reply = replySentWith(endpoint);
        then(reply).should().setFailed(eq(10021), any());
        then(reply).should().setContext(0, null);
        then(billing).shouldHaveNoInteractions();
    }

    @Test
    public void shouldDoNothingOnPoolDownUnrelatedToOngoingPool2Pool() throws Exception
    {
        given(aPartitionManager().withDefault(aPartition()));
        given(aPoolSelectionUnit().withNetUnit("all-net", "192.168.1.1")
                .withProtocolUnit("HTTP", "http/1"));
        given(aPoolMonitor().thatReturns(aPoolSelectorThat()
                .onPool2PoolSelects(aPoolPair()
                        .withSource("source-pool@dCacheDomain")
                        .withDestination("destination-pool@dCacheDomain"))));
        given(aContainer("PoolManager@dCacheDomain").thatDoesNotSendHitMessages());
        given(aReplicateRequest()
                .forFile("80D1B8B90CED30430608C58002811B3285FC")
                .withPath("/public/test")
                .withFileAttributes(attributes().size(10, KiB).storageInfo(aStorageInfo()))
                .withProtocolInfo(aProtocolInfo().withProtocol("http")
                        .withMajorVersion(1).withIPAddress("192.168.1.1")));
        verify(endpoint).sendMessage(any());

        whenReceiving(aPoolStatusChange().thatPool("unrelated-pool").isDown());

        then(endpoint).shouldHaveNoMoreInteractions();
        then(billing).shouldHaveNoInteractions();
    }

    @Test
    public void shouldRetryOnStagePoolUp() throws Exception
    {
        var stagePool = aPool("stage-pool@dCacheDomain");
        given(aPartitionManager().withDefault(aPartition().withStageAllowed(true)));
        given(aPoolSelectionUnit().withNetUnit("all-net", "192.168.1.1")
                .withProtocolUnit("HTTP", "http/1"));
        given(aPoolMonitor().thatReturns(aPoolSelectorThat()
                .onReadThrows(aFileNotInCacheException())
                .onStageSelects(stagePool)));
        given(aContainer("PoolManager@dCacheDomain").thatDoesNotSendHitMessages());
        given(aReadRequest()
                .by(ROOT)
                .forFile("80D1B8B90CED30430608C58002811B3285FC")
                .withPath("/public/test")
                .withFileAttributes(attributes().size(10, KiB)
                        .storageInfo(aStorageInfo().withLocation("osm://RZ1/bfid1")))
                .withProtocolInfo(aProtocolInfo().withProtocol("http")
                        .withMajorVersion(1).withIPAddress("192.168.1.1")));

        whenReceiving(aPoolStatusChange().thatPool("stage-pool").isUp());

        var reply = replySentWith(endpoint);
        then(reply).should().setFailed(eq(10021), any());
        then(reply).should().setContext(0, stagePool);
        then(billing).shouldHaveNoInteractions();
    }

    @Test
    public void shouldDoNothingOnPoolUpUnrelatedToOngoingStage() throws Exception
    {
        given(aPartitionManager().withDefault(aPartition().withStageAllowed(true)));
        given(aPoolSelectionUnit().withNetUnit("all-net", "192.168.1.1")
                .withProtocolUnit("HTTP", "http/1"));
        given(aPoolMonitor().thatReturns(aPoolSelectorThat()
                .onReadThrows(aFileNotInCacheException())
                .onStageSelects("stage-pool@dCacheDomain")));
        given(aContainer("PoolManager@dCacheDomain").thatDoesNotSendHitMessages());
        given(aReadRequest()
                .by(ROOT)
                .forFile("80D1B8B90CED30430608C58002811B3285FC")
                .withPath("/public/test")
                .withFileAttributes(attributes().size(10, KiB)
                        .storageInfo(aStorageInfo().withLocation("osm://RZ1/bfid1")))
                .withProtocolInfo(aProtocolInfo().withProtocol("http")
                        .withMajorVersion(1).withIPAddress("192.168.1.1")));
        verify(endpoint).sendMessage(any());

        whenReceiving(aPoolStatusChange().thatPool("unrelated-pool").isUp());

        then(endpoint).shouldHaveNoMoreInteractions();
        then(billing).shouldHaveNoInteractions();
    }

    @Test
    public void shouldRetryOnStagePoolDown() throws Exception
    {
        var stagePool = aPool("stage-pool@dCacheDomain");
        given(aPartitionManager().withDefault(aPartition().withStageAllowed(true)));
        given(aPoolSelectionUnit().withNetUnit("all-net", "192.168.1.1")
                .withProtocolUnit("HTTP", "http/1"));
        given(aPoolMonitor().thatReturns(aPoolSelectorThat()
                .onReadThrows(aFileNotInCacheException())
                .onStageSelects(stagePool)));
        given(aContainer("PoolManager@dCacheDomain").thatDoesNotSendHitMessages());
        given(aReadRequest()
                .by(ROOT)
                .forFile("80D1B8B90CED30430608C58002811B3285FC")
                .withPath("/public/test")
                .withFileAttributes(attributes().size(10, KiB)
                        .storageInfo(aStorageInfo().withLocation("osm://RZ1/bfid1")))
                .withProtocolInfo(aProtocolInfo().withProtocol("http")
                        .withMajorVersion(1).withIPAddress("192.168.1.1")));

        whenReceiving(aPoolStatusChange().thatPool("stage-pool").isDown());

        var reply = replySentWith(endpoint);
        then(reply).should().setFailed(eq(10021), any());
        then(reply).should().setContext(0, stagePool);
        then(billing).shouldHaveNoInteractions();
    }

    @Test
    public void shouldDoNothingOnPoolDownUnrelatedToOngoingStage() throws Exception
    {
        given(aPartitionManager().withDefault(aPartition().withStageAllowed(true)));
        given(aPoolSelectionUnit().withNetUnit("all-net", "192.168.1.1")
                .withProtocolUnit("HTTP", "http/1"));
        given(aPoolMonitor().thatReturns(aPoolSelectorThat()
                .onReadThrows(aFileNotInCacheException())
                .onStageSelects("stage-pool@dCacheDomain")));
        given(aContainer("PoolManager@dCacheDomain").thatDoesNotSendHitMessages());
        given(aReadRequest()
                .by(ROOT)
                .forFile("80D1B8B90CED30430608C58002811B3285FC")
                .withPath("/public/test")
                .withFileAttributes(attributes().size(10, KiB)
                        .storageInfo(aStorageInfo().withLocation("osm://RZ1/bfid1")))
                .withProtocolInfo(aProtocolInfo().withProtocol("http")
                        .withMajorVersion(1).withIPAddress("192.168.1.1")));
        verify(endpoint).sendMessage(any());

        whenReceiving(aPoolStatusChange().thatPool("unrelated-pool").isDown());

        then(endpoint).shouldHaveNoMoreInteractions();
        then(billing).shouldHaveNoInteractions();
    }

    @Test
    public void shouldDoNothingOnPoolDownWhenRequestIsSuspended() throws Exception
    {
        given(aPartitionManager().withDefault(aPartition()));
        given(aPoolSelectionUnit().withNetUnit("all-net", "192.168.1.1")
                .withProtocolUnit("HTTP", "http/1"));
        given(aPoolMonitor().thatReturns(aPoolSelectorThat()
                .onReadThrows(aFileNotInCacheException())));
        given(aContainer("PoolManager@dCacheDomain")
                .thatDoesNotSendHitMessages()
                .withConfig("rc onerror suspend"));
        given(aReadRequest()
                .by(ROOT)
                .forFile("80D1B8B90CED30430608C58002811B3285FC")
                .withPath("/public/test")
                .withFileAttributes(attributes().size(10, KiB).storageInfo(aStorageInfo()))
                .withProtocolInfo(aProtocolInfo().withProtocol("http")
                        .withMajorVersion(1).withIPAddress("192.168.1.1")));

        whenReceiving(aPoolStatusChange().thatPool("some-pool").isDown());

        then(endpoint).shouldHaveNoInteractions();
    }

    @Test
    public void shouldRetryOnPoolUpWhenRequestIsSuspended() throws Exception
    {
        given(aPartitionManager().withDefault(aPartition()));
        given(aPoolSelectionUnit().withNetUnit("all-net", "192.168.1.1")
                .withProtocolUnit("HTTP", "http/1"));
        given(aPoolMonitor().thatReturns(aPoolSelectorThat()
                .onReadThrows(aFileNotInCacheException())));
        given(aContainer("PoolManager@dCacheDomain")
                .thatDoesNotSendHitMessages()
                .withConfig("rc onerror suspend"));
        given(aReadRequest()
                .by(ROOT)
                .forFile("80D1B8B90CED30430608C58002811B3285FC")
                .withPath("/public/test")
                .withFileAttributes(attributes().size(10, KiB).storageInfo(aStorageInfo()))
                .withProtocolInfo(aProtocolInfo().withProtocol("http")
                        .withMajorVersion(1).withIPAddress("192.168.1.1")));

        whenReceiving(aPoolStatusChange().thatPool("some-pool").isUp());

        var reply = replySentWith(endpoint);
        then(reply).should().setFailed(eq(10021), any());
        then(reply).should().setContext(0, null);
        then(endpoint).shouldHaveNoMoreInteractions();
    }

    private void given(ContainerBuilder builder)
    {
        container = builder.build();
    }

    private void given(PoolMonitorBuilder builder)
    {
        poolMonitor = builder.build();
    }

    private void given(PoolSelectionUnitBuilder builder)
    {
        psu = builder.build();
    }

    private void given(EndpointMessageReceiver receiver)
    {
        receiver.build();
    }

    private void given(PartitionManagerBuilder builder)
    {
        partitionManager = builder.build();
    }

    private String given(Deliverable builder)  throws IOException,
            InterruptedException, CommandException
    {
        builder.deliverTo(container);

        return doAdminCommand("rc ls").split(" ")[0];
    }

    private void given(DeliveryRepeater repeater, IntFunction<Deliverable> messageBuilder)
    {
        repeater.repeatDeliveryTo(messageBuilder, container);
    }

    private void whenReceiving(Deliverable builder) throws IOException, InterruptedException
    {
        builder.deliverTo(container);
        waitUntilQuiescent();
    }

    private String doAdminCommand(String command) throws CommandException
    {
        CommandInterpreter interpreter = new CommandInterpreter(container);
        Serializable response = interpreter.command(new Args(command));
        return response == null ? "" : response.toString();
    }

    private void whenExecutedAdminCommand(String command) throws CommandException,
            InterruptedException
    {
        commandResponse = doAdminCommand(command);
        waitUntilQuiescent();
    }

    private void whenPingSend() throws InterruptedException
    {
        container.start();
        try {
            container.pingAllPools();
        } finally {
            container.shutdown();
        }
        waitUntilQuiescent();
    }

    private static CellMessage envelopeSentWith(CellEndpoint endpointUsed)
    {
        var envelopeArg = ArgumentCaptor.forClass(CellMessage.class);
        verify(endpointUsed, Mockito.atLeastOnce()).sendMessage(envelopeArg.capture());
        return envelopeArg.getValue();
    }

    private static PoolMgrSelectReadPoolMsg replySentWith(CellEndpoint endpointUsed)
    {
        var envelope = envelopeSentWith(endpointUsed);
        return (PoolMgrSelectReadPoolMsg) envelope.getMessageObject();
    }

    private static List<PoolMgrSelectReadPoolMsg> allRepliesSentWith(CellEndpoint endpointUsed)
    {
        var envelopeArg = ArgumentCaptor.forClass(CellMessage.class);
        verify(endpointUsed, Mockito.atLeastOnce()).sendMessage(envelopeArg.capture());
        return envelopeArg.getAllValues().stream()
                .map(CellMessage::getMessageObject)
                .filter(PoolMgrSelectReadPoolMsg.class::isInstance)
                .map(PoolMgrSelectReadPoolMsg.class::cast)
                .collect(Collectors.toList());
    }

    private static diskCacheV111.vehicles.Pool poolSetInMessage(PoolMgrSelectReadPoolMsg message)
    {
        var poolArg = ArgumentCaptor.forClass(diskCacheV111.vehicles.Pool.class);
        verify(message).setPool(poolArg.capture());
        return poolArg.getValue();
    }

    private static List<RestoreHandlerInfo> allRestoreHandlerInfo(PoolManagerGetRestoreHandlerInfo response)
    {
        var list = ArgumentCaptor.forClass(List.class);
        verify(response).setResult(list.capture());
        return list.getValue();
    }

    private static void deliverToContainer(RequestContainerV5 container,
                                           CellMessage envelope,
                                           Serializable request)
        throws IOException {
            /* REVISIT: the following embeds knowledge of how RequestContainerV5
               receives information; specifically, that messages of type
               PoolMgrSelectReadPoolMsg are handled differently from other
               messages.  This is undesirable, as changes in how
               RequestContainerV5 receives messages will require similar
               changes here.  However, the alternative (using reflection) was
               felt to be too complicated.  A future version may replace this
               by using dCache's actual message delivery mechanism here.
            */
        if (request instanceof PoolMgrSelectReadPoolMsg) {
            container.messageArrived(envelope, (PoolMgrSelectReadPoolMsg)request);
        } else {
            container.messageArrived(envelope, request);
        }
    }

    private static <T extends InfoMessage> T notificationSentWith(CellStub stub, Class<T> type)
    {
        var messageArg = ArgumentCaptor.forClass(Serializable.class);
        verify(stub, Mockito.atLeastOnce()).notify(messageArg.capture());
        return messageArg.getAllValues().stream()
                .filter(e -> type.isInstance(e))
                .map(type::cast)
                .findFirst()
                .orElseThrow(() -> new AssertionError("CellStub#notify not called with "
                    + type + " argument"));
    }

    private FixedTimes repeat(int count)
    {
        return new FixedTimes<RequestContainerV5>(count);
    }

    private EndpointMessageReceiver aCell(String address)
    {
        return new ContainerEndpointMessageReceiver(address);
    }

    private ContainerBuilder aContainer(String address)
    {
        return new ContainerBuilder(address);
    }

    private SelectReadPoolRequestBuilder aReadRequest()
    {
        return new SelectReadPoolRequestBuilder();
    }

    private SelectReadPoolRequestBuilder aReadRequestFrom(String doorAddress)
    {
        return new SelectReadPoolRequestBuilder(doorAddress);
    }

    private ReplicateFileRequestBuilder aReplicateRequest()
    {
        return new ReplicateFileRequestBuilder();
    }

    private ResponseMessageDeliverable aResponseTo(Future<CellMessage> outbound)
            throws InterruptedException, ExecutionException
    {
        if (!outbound.isDone()) {
            fail("aResponseTo() called with Future<CellMessage> argument that is not yet done.");
        }

        return new ContainerResponseMessageDeliverable(outbound.get());
    }

    private PoolStatusChangedBuilder aPoolStatusChange()
    {
        return new ContainerPoolStatusChangedBuilder();
    }

    private Deliverable anInfoRequest()
    {
        return new GetRestoreHandlerInfoRequestBuilder();
    }

    private CostExceptionBuilder aCostException()
    {
        return new CostExceptionBuilder();
    }

    private FileNotInCacheException aFileNotInCacheException()
    {
        return new FileNotInCacheException("file not on any pool");
    }

    private PermissionDeniedCacheException aPermissionDeniedCacheException()
    {
        return new PermissionDeniedCacheException("read access not allowed");
    }

    private CacheException aCacheException(int rc)
    {
        return new CacheException(rc, "simulating an arbitrary problem");
    }

    private CacheException aCacheException()
    {
        return new CacheException("simulating an arbitrary problem");
    }

    private IllegalArgumentException anIllegalArgumentException()
    {
        return new IllegalArgumentException("simulating a bad configuration");
    }

    private RuntimeException aRuntimeException()
    {
        return new RuntimeException("simulating a bug");
    }

    private SourceCostException aSourceCostException()
    {
        return new SourceCostException("source pool is hot");
    }

    private DestinationCostException aDestinationCostException()
    {
        return new DestinationCostException("destination pool is hot");
    }

    /**
     * Build the ContainerBuilderV5 instance we wish to test.  Various
     * configuration options normally injected via Spring are represented
     * through a fluent interface.
     */
    private class ContainerBuilder
    {
        private final CellAddressCore address;

        private boolean hitMessages;
        private List<String> commands = Collections.emptyList();

        public ContainerBuilder(String address)
        {
            this.address = new CellAddressCore(address);
        }

        public ContainerBuilder thatSendsHitMessages()
        {
            hitMessages = true;
            return this;
        }

        public ContainerBuilder thatDoesNotSendHitMessages()
        {
            hitMessages = false;
            return this;
        }

        public ContainerBuilder withConfig(String... lines)
        {
            commands = Arrays.asList(lines);
            return this;
        }

        public RequestContainerV5 build()
        {
            RequestContainerV5 container = new FriendlyRequestContainerV5();

            container.setBilling(billing);
            container.setPoolStub(pool);
            container.setPnfsHandler(pnfs);
            container.setCellEndpoint(endpoint);
            container.setCellAddress(address);
            container.setStageConfigurationFile(null);

            container.setPoolSelectionUnit(requireNonNull(psu));
            container.setPoolMonitor(requireNonNull(poolMonitor));
            container.setPartitionManager(requireNonNull(partitionManager));
            container.setHitInfoMessages(hitMessages);
            container.setExecutor(executor);

            CommandInterpreter interpreter = new CommandInterpreter(container);

            commands.forEach(c -> {
                        try {
                            String response = interpreter.command(c);
                            LOGGER.debug("Command \"{}\" yielded response {}",
                                    c, response);
                        } catch (CommandException | RuntimeException e) {
                            fail("Command \"" + c + "\" failed: " + e);
                        }
                    });

            return container;
        }
    }

    private class ContainerEndpointMessageReceiver
        extends EndpointMessageReceiver<RequestContainerV5>
    {
        protected ContainerEndpointMessageReceiver(String address) {
            super(address);
        }

        @Override
        protected ResponseMessageDeliverable<RequestContainerV5> aResponseTo(CellMessage message) {
            return new ContainerResponseMessageDeliverable(message);
        }
    }

    /**
     * This class implements the builder pattern for creating a mocked
     * CostException.
     */
    private class CostExceptionBuilder implements CacheExceptionBuilder<CostException>
    {
        private SelectedPool bestPool;
        private boolean shouldTryAlternatives;

        public CostExceptionBuilder withPool(String address)
        {
            bestPool = aPool(address);
            return this;
        }

        public CostExceptionBuilder withPool(SelectedPool pool)
        {
            bestPool = pool;
            return this;
        }

        public CostExceptionBuilder withTryAlternatives(boolean enable)
        {
            shouldTryAlternatives = enable;
            return this;
        }

        @Override
        public CostException build()
        {
            return new CostException("cost exceeded", bestPool, false,
                    shouldTryAlternatives);
        }
    }

    private static class ContainerPoolStatusChangedBuilder
        extends PoolStatusChangedBuilder<RequestContainerV5>
    {
        @Override
        public void deliverTo(RequestContainerV5 container)
        {
            container.poolStatusChanged(pool, status);
        }
    }

    /**
     * Deliver a PoolMgrReplicateFileMsg to RequestContainerV5 using the
     * builder pattern.  The Hopping Manager makes use of this message to
     * request that RequestContainerV5 makes additional copies of a file.
     */
    private class ReplicateFileRequestBuilder extends SelectReadPoolRequestBuilder
    {
        private boolean allowRestore;
        private boolean isAllowRestoreSet;
        private int fileStatus;
        private boolean isFileStatusSet;

        public ReplicateFileRequestBuilder allowRestore(boolean value)
        {
            allowRestore = value;
            isAllowRestoreSet = true;
            return this;
        }

        public ReplicateFileRequestBuilder withDestinationFileStatus(int status)
        {
            fileStatus = status;
            isFileStatusSet = true;
            return this;
        }

        @Override
        protected PoolMgrReplicateFileMsg newMock()
        {
            return mock(PoolMgrReplicateFileMsg.class);
        }

        @Override
        protected PoolMgrSelectReadPoolMsg buildRequest()
        {
            PoolMgrReplicateFileMsg request =
                    (PoolMgrReplicateFileMsg) super.buildRequest();

            if (isAllowRestoreSet) {
                BDDMockito.given(request.allowRestore()).willReturn(allowRestore);
            }

            if (isFileStatusSet) {
                BDDMockito.given(request.getDestinationFileStatus()).willReturn(fileStatus);
            }

            return request;
        }
    }

    /**
     * This class delivers a request to RequestContainerV5.  It is also
     * responsible that the {@literal deliverTo} method does not return until
     * RequestContainerV5 has fully processed the request.  This should result
     * in RequestContainerV5 either sending a reply or sending a message to
     * a pool.
     */
    private abstract class RequestMessageDeliverable
        extends EnvelopeAndMessageDeliverable<RequestContainerV5>
    {
        RequestMessageDeliverable(String source)
        {
            this.sourceAddress = new CellAddressCore(source);
        }

        protected abstract Serializable buildRequest();

        @Override
        protected CellMessage buildEnvelope()
        {
            Serializable request = buildRequest();

            CellAddressCore destination = ((FriendlyRequestContainerV5)container).getCellAddress();

            CellMessage msg = new CellMessage(destination, request);
            msg.addSourceAddress(sourceAddress);
            return msg;
        }

        @Override
        protected void doDeliverTo(RequestContainerV5 receiver, CellMessage envelope,
            Serializable request) throws IOException, InterruptedException {
            deliverToContainer(container, envelope, request);
            waitUntilQuiescent();
        }
    }

    /**
     * Send a reply from a pool to RequestContainerV5.  This is done by the
     * same thread sending the request.
     */
    private class ContainerResponseMessageDeliverable
        extends ResponseMessageDeliverable<RequestContainerV5>
    {
        protected ContainerResponseMessageDeliverable(CellMessage outbound)
        {
            super(outbound);
        }

        @Override
        public ResponseMessageDeliverable aResponseTo(CellMessage message) {
            return new ContainerResponseMessageDeliverable(message);
        }

        @Override
        protected void configureOutboundAddress() {
            this.sourceAddress = ((FriendlyRequestContainerV5)container).getCellAddress();
        }

        @Override
        protected void doDeliverTo(RequestContainerV5 receiver, CellMessage envelope,
            Serializable request) throws IOException, InterruptedException {
            deliverToContainer(container, envelope, request);
        }
    }

    /**
     * Deliver a PoolMgrSelectReadPoolMsg to RequestContainerV5 using the
     * builder pattern.  This is the primary way of triggering activity within
     * RequestContainerV5.
     */
    private class SelectReadPoolRequestBuilder extends RequestMessageDeliverable
    {
        private final EnumSet<RequestState> allowedStates = EnumSet.allOf(RequestState.class);

        private ProtocolInfo protocolInfo;
        private PnfsId file;
        private FileAttributes fileAttributes;
        private String billingPath;
        private String transferPath;
        private Subject user = Subjects.NOBODY;

        private SelectReadPoolRequestBuilder()
        {
            super("door@dCacheDomain");
        }

        private SelectReadPoolRequestBuilder(String doorAddress)
        {
            super(doorAddress);
        }

        public SelectReadPoolRequestBuilder withStaging(boolean allowed)
        {
            if (allowed) {
                allowedStates.add(RequestState.ST_STAGE);
            } else {
                allowedStates.remove(RequestState.ST_STAGE);
            }
            return this;
        }

        public SelectReadPoolRequestBuilder withBillingPath(String path)
        {
            billingPath = path;
            return this;
        }

        public SelectReadPoolRequestBuilder withTransferPath(String path)
        {
            transferPath = path;
            return this;
        }

        public SelectReadPoolRequestBuilder withPath(String path)
        {
            billingPath = path;
            transferPath = path;
            return this;
        }

        public SelectReadPoolRequestBuilder withPool2Pool(boolean allowed)
        {
            if (allowed) {
                allowedStates.add(RequestState.ST_POOL_2_POOL);
            } else {
                allowedStates.remove(RequestState.ST_POOL_2_POOL);
            }
            return this;
        }

        public SelectReadPoolRequestBuilder withProtocolInfo(ProtocolInfoBuilder infoBuilder)
        {
            return withProtocolInfo(infoBuilder.build());
        }

        public SelectReadPoolRequestBuilder withProtocolInfo(ProtocolInfo info)
        {
            protocolInfo = info;
            return this;
        }

        public SelectReadPoolRequestBuilder withFileAttributes(FileAttributesBuilder builder)
        {
            return withFileAttributes(builder.build());
        }

        public SelectReadPoolRequestBuilder withFileAttributes(FileAttributes attributes)
        {
            fileAttributes = attributes;
            if (file != null) {
                fileAttributes.setPnfsId(file);
            }
            return this;
        }

        public SelectReadPoolRequestBuilder forFile(String id)
        {
            file = new PnfsId(id);
            if (fileAttributes != null) {
                fileAttributes.setPnfsId(file);
            }
            return this;
        }

        public SelectReadPoolRequestBuilder by(Subject user)
        {
            this.user = user;
            return this;
        }

        protected PoolMgrSelectReadPoolMsg newMock()
        {
            return mock(PoolMgrSelectReadPoolMsg.class);
        }

        @Override
        protected PoolMgrSelectReadPoolMsg buildRequest()
        {
            PoolMgrSelectReadPoolMsg request = newMock();
            BDDMockito.given(request.getPnfsId()).willReturn(file);
            BDDMockito.given(request.getPoolGroup()).willReturn(requireNonNull(POOL_GROUP));
            BDDMockito.given(request.getProtocolInfo()).willReturn(requireNonNull(protocolInfo));
            BDDMockito.given(request.getAllowedStates()).willReturn(allowedStates);
            BDDMockito.given(request.getFileAttributes()).willReturn(requireNonNull(fileAttributes));
            BDDMockito.given(request.getContext()).willReturn(new Context());
            BDDMockito.given(request.getBillingPath()).willReturn(requireNonNull(billingPath));
            BDDMockito.given(request.getTransferPath()).willReturn(requireNonNull(transferPath));
            BDDMockito.given(request.getSubject()).willReturn(requireNonNull(user));
            return request;
        }
    }

    private class GetRestoreHandlerInfoRequestBuilder implements Deliverable<RequestContainerV5>
    {
        @Override
        public void deliverTo(RequestContainerV5 container) throws IOException
        {
            PoolManagerGetRestoreHandlerInfo request = mock(PoolManagerGetRestoreHandlerInfo.class);

            infoResponse = container.messageArrived(request);
        }
    }

    /**
     * Wait until the RequestContainer is no longer processing input.
     *
     * The state-updating task will only exit if the read request:
     *
     *      is waiting for a pool2pool transfer to complete,
     *
     *      is waiting for a stage request to complete,
     *
     *      has been suspended,
     *
     *      has fully processed the request (replies sent to the doors).
     *
     *  Therefore, provided the state-updating task has been submitted
     *  before calling this method, it is guaranteed that any messages have
     *  been sent when this method exists.
     *
     * Since the executor is single-threaded, it is guaranteed that,
     * when this method returns, the container will have sent
     * any pending messages.
     */
    private void waitUntilQuiescent() throws InterruptedException
    {
        ExecutorUtils.waitUntilQuiescent(executor);
    }

    /**
     * A subclass of RequestContainerV5 that allows the testing code to
     * obtain information it injected.  This class is ONLY here as a work-around
     * for a setter/getter asymmetry, where the setter method is public but
     * the getter method is protected.  Specifically, it SHOULD NOT be used to
     * spy on the class's internal state!
     */
    private class FriendlyRequestContainerV5 extends RequestContainerV5
    {
        @Override
        public CellAddressCore getCellAddress()
        {
            return super.getCellAddress();
        }
    }
}
