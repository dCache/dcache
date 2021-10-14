package org.dcache.services.info.base;


import static org.junit.Assert.assertEquals;

import org.junit.Test;


public class BadStatePathExceptionTests {

    @Test
    public void testBadStatePathException() {
        BadStatePathException e = new BadStatePathException();

        assertEquals("Message not default one", BadStatePathException.DEFAULT_MESSAGE,
              e.toString());
    }

    @Test
    public void testBadStatePathExceptionString() {
        String msg = new String("the message");
        Exception e = new BadStatePathException(msg);

        assertEquals("Message stored in constructor not as expected", msg, e.toString());
    }

}
