package org.dcache.services.info.base;


import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class NoMetricStatePathExceptionTest {

	private static final String ERROR_MESSAGE = "test error message";

	NoMetricStatePathException _exception = new NoMetricStatePathException(ERROR_MESSAGE);

	@Test
	public void testToString() {
		StringBuilder sb = new StringBuilder();

		sb.append( NoMetricStatePathException.MESSAGE_PREFIX);
		sb.append( ERROR_MESSAGE);

		assertEquals( "error message wrong", sb.toString(), _exception.toString());
	}

}
