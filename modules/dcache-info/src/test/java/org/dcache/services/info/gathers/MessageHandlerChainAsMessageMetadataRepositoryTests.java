package org.dcache.services.info.gathers;


import org.junit.Before;
import org.junit.Test;

import dmg.cells.nucleus.UOID;

import static org.junit.Assert.*;

/**
 * Some tests that check the MessageHandlerChain's implementation of
 * the MessageMetadataRepository Interface
 */
public class MessageHandlerChainAsMessageMetadataRepositoryTests {

    final static long AVAILABLE_TTL = 2;
    final static UOID AVAILABLE_ID = new UOID();

    MessageMetadataRepository<UOID> _metadataRepo;

    @Before
    public void setUp()
    {
        _metadataRepo = new MessageHandlerChain();
        _metadataRepo.putMetricTTL( AVAILABLE_ID, AVAILABLE_TTL);
    }

    @Test( expected=NullPointerException.class)
    public void testPutNull() {
        _metadataRepo.putMetricTTL( null, 3);
        fail( "Unexpected absence of NullPointerException");
    }


    @Test( expected=IllegalArgumentException.class)
    public void testGetNotPreset() {
        UOID id = new UOID();
        _metadataRepo.getMetricTTL( id);
        fail( "Unexpected absence of IllegalArguemtnException");
    }

    @Test
    public void testGetPresent() {
        long getTtl = _metadataRepo.getMetricTTL( AVAILABLE_ID);
        assertEquals( "getMetricTTL returned value", AVAILABLE_TTL, getTtl);
    }

    @Test
    public void testContainsPresentMessageTTL() {
        assertTrue( "checking containsMetricTTL for available message ID", _metadataRepo.containsMetricTTL( AVAILABLE_ID));
    }

    @Test
    public void testContainsNonPresentMessageTTL() {
        assertFalse( "checking containsMetricTTL for absent message ID", _metadataRepo.containsMetricTTL( new UOID()));
    }

    @Test
    public void testRemoveAvailableMessage() {
        _metadataRepo.remove( AVAILABLE_ID);
        assertFalse( "Checking availablity after remove", _metadataRepo.containsMetricTTL( AVAILABLE_ID));
    }

    @Test( expected=IllegalArgumentException.class)
    public void testRemoveNonPresentMessageTTL() {
        _metadataRepo.remove(  new UOID());
        fail( "Unexpected absence of IllegalArgumentException");
    }
}
