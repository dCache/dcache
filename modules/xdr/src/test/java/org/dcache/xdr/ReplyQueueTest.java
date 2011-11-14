/*
 * This library is free software; you can redistribute it and/or modify
 * it under the terms of the GNU Library General Public License as
 * published by the Free Software Foundation; either version 2 of the
 * License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Library General Public License for more details.
 *
 * You should have received a copy of the GNU Library General Public
 * License along with this program (see the file COPYING.LIB for more
 * details); if not, write to the Free Software Foundation, Inc.,
 * 675 Mass Ave, Cambridge, MA 02139, USA.
 */

package org.dcache.xdr;

import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;


public class ReplyQueueTest {

    private ReplyQueue<Integer, String> _replyQueue;

    @Before
    public void setUp() {
        _replyQueue = new ReplyQueue<Integer, String>();
    }

    @Test
    public void testPutNoRegistration() {
        _replyQueue.put(1, "test");
    }

    @Test(expected=IllegalArgumentException.class)
    public void testGetNotRegistered() throws Exception {
        _replyQueue.get(1);
        fail("exception not thrown");
    }

    public void testGetNotPutted() throws Exception {
        _replyQueue.registerKey(1);
        String result = _replyQueue.get(1, 20);
        assertNull("Not null as timeout", result);
    }

    @Test(timeout=500)
    public void testNotToBlockOnExistance() throws InterruptedException {
        _replyQueue.registerKey(1);
        _replyQueue.put(1, "bla");
        String result = _replyQueue.get(1, 5000);
        assertNotNull("Did not got in time", result);
    }

}