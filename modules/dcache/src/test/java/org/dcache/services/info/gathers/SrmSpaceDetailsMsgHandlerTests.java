package org.dcache.services.info.gathers;

import org.junit.Before;
import org.junit.Test;

import diskCacheV111.services.space.message.GetSpaceTokensMessage;

public class SrmSpaceDetailsMsgHandlerTests {

    SrmSpaceDetailsMsgHandler _handler;

    @Before
    public void setup() {
        _handler = new SrmSpaceDetailsMsgHandler( null );
    }

    @Test
    public void shouldHandleMessageWithUninitializedSpaceTokens() {
        assert _handler.handleMessage( new GetSpaceTokensMessage(), 0 );
    }
}
