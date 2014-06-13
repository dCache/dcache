package org.dcache.services.info.gathers;

import org.junit.Before;
import org.junit.Test;

import diskCacheV111.services.space.message.GetLinkGroupsMessage;

public class LinkgroupDetailsMsgHandlerTests {

    LinkgroupDetailsMsgHandler _handler;

    @Before
    public void setup() {
        _handler = new LinkgroupDetailsMsgHandler( null );
    }

    @Test
    public void shouldHandleMessageWithUninitializedLinkGroups() {
        assert _handler.handleMessage( new GetLinkGroupsMessage(), 0 );
    }
}
