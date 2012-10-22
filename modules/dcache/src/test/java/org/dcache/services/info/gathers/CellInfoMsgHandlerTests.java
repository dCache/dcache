package org.dcache.services.info.gathers;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Queue;
import java.util.TimeZone;

import org.dcache.services.info.base.IntegerStateValue;
import org.dcache.services.info.base.QueuingStateUpdateManager;
import org.dcache.services.info.base.StatePath;
import org.dcache.services.info.base.StateUpdate;
import org.dcache.services.info.base.StringStateValue;
import org.dcache.services.info.gathers.cells.CellInfoMsgHandler;
import org.junit.Before;
import org.junit.Test;

import dmg.cells.nucleus.CellInfo;
import dmg.cells.nucleus.CellVersion;
import dmg.cells.nucleus.InitialisableCellInfo;
import dmg.cells.nucleus.UOID;

public class CellInfoMsgHandlerTests {

    private final static DateFormat SIMPLE_DATE_FORMAT =
            new SimpleDateFormat( "MMM d, HH:mm:ss z");
    private final static DateFormat ISO8601_DATE_FORMAT =
            new SimpleDateFormat( "yyyy-MM-dd'T'HH:mm'Z'");

    static {
        ISO8601_DATE_FORMAT.setTimeZone( TimeZone.getTimeZone( "GMT"));
    }

    /**
     * Provide a dummy implementation of metadata repository. It supports
     * only a single message.
     */
    private static class DummyMetadataRepo<ID> implements
            MessageMetadataRepository<ID> {
        private ID _msgId;
        private long _ttl;

        @Override
        public boolean containsMetricTTL( ID messageId) {
            return _msgId != null ? _msgId.equals( messageId) : false;
        }

        @Override
        public long getMetricTTL( ID messageId) {
            if( messageId == null || !messageId.equals( _msgId)) {
                throw new IllegalArgumentException("no msg with that ID");
            }
            return _ttl;
        }

        @Override
        public void putMetricTTL( ID messageId, long ttl) {
            _msgId = messageId;
            _ttl = ttl;
        }

        @Override
        public void remove( ID messageId) {
            _msgId = null;
        }
    }

    QueuingStateUpdateManager _sum;
    CellInfoMsgHandler _handler;
    MessageMetadataRepository<UOID> _metadataRepo;

    @Before
    public void setUp()
    {
        _sum = new QueuingStateUpdateManager();
        _metadataRepo = new DummyMetadataRepo<>();
        _handler = new CellInfoMsgHandler( _sum, _metadataRepo);
    }

    @Test
    public void testProcessNull() {
        _handler.process( null, 10);
        assertEquals( "checking StateUpdateManager's queue size", 0, _sum.getQueue().size());
    }

    @Test
    public void testReceivedException() {
        _handler.process( new RuntimeException( "me bad"), 10);
        assertEquals( "checking StateUpdateManager's queue size", 0, _sum.getQueue().size());
    }

    @Test
    public void testReceivedCellInfo() {
        CellInfo info[] = new CellInfo[1];
        String version = "version";
        String release = "release";
        String cellName = "cell-1";
        String cellType = "cell-1-type";
        String cellClass = "cell-1-class";
        String domainName = "domain-1";
        Date creationTime = new Date();
        String privateInfo = "cell-1-private";
        String shortInfo = "cell-1-short";
        int eventQueueSize = 3;
        int threadCount = 5;
        int state = 2;

        CellVersion cellVersion = new CellVersion( version, release);

        info[0] =
                new InitialisableCellInfo( cellName, cellType, cellClass,
                                           cellVersion, domainName,
                                           creationTime, privateInfo,
                                           shortInfo, eventQueueSize,
                                           threadCount, state);

        info[0].setCellVersion( new CellVersion( version, release));

        _handler.process( info, 10);

        Queue<StateUpdate> updates = _sum.getQueue();

        assertEquals( "checking StateUpdateManager's queue size", 1, updates.size());

        assertCellInfo( updates.poll(), info[0]);
    }

    /**
     * Assert that the supplied StateUpdate object contains fresh values for
     * the expected metrics for the given CellInfo object.
     * 
     * @param update
     * @param info
     */
    private static void assertCellInfo( StateUpdate update, CellInfo info) {
        assertEquals( "expect number of purges", 0, update.countPurges());

        assertEquals( "expected number of metric updates", 9, update.count());

        StatePath cellPath =
                StatePath.parsePath( "domains").newChild( info.getDomainName())
                        .newChild( "cells").newChild( info.getCellName());

        assertTrue( "Check class metric added", update
                .hasUpdate( cellPath.newChild( "class"),
                        new StringStateValue( info.getCellClass())));
        assertTrue( "Check type metric added", update.hasUpdate( cellPath
                .newChild( "type"), new StringStateValue( info.getCellType())));

        assertTrue( "Check event-queue-size metric added", update.hasUpdate(
                cellPath.newChild( "event-queue-size"),
                new IntegerStateValue( info.getEventQueueSize())));
        assertTrue( "Check thread-count metric added", update.hasUpdate(
                cellPath.newChild( "thread-count"), new IntegerStateValue( info
                        .getThreadCount())));

        // Three metrics under the "created" branch
        StatePath createdPath = cellPath.newChild( "created");
        assertTrue( "Check created.unix metric added", update.hasUpdate(
                createdPath.newChild( "unix"), new IntegerStateValue( info
                        .getCreationTime().getTime() / 1000)));
        assertTrue( "Check created.simple metric added", update.hasUpdate(
                createdPath.newChild( "simple"),
                new StringStateValue( SIMPLE_DATE_FORMAT.format( info
                        .getCreationTime()))));
        assertTrue( "Check created.ISO-8601 metric added", update.hasUpdate(
                createdPath.newChild( "ISO-8601"),
                new StringStateValue( ISO8601_DATE_FORMAT.format( info
                        .getCreationTime()))));

        // Two metrics under the "version" branch
        StatePath versionPath = cellPath.newChild( "version");
        CellVersion version = info.getCellVersion();
        assertTrue( "Check version.revision metric added", update.hasUpdate(
                versionPath.newChild( "revision"),
                new StringStateValue( version.getRevision())));
        assertTrue( "Check version.release metric added", update.hasUpdate(
                versionPath.newChild( "release"), new StringStateValue( version
                        .getRelease())));
    }

}
