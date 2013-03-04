package org.dcache.services.info.base;


import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class MetricStatePathExceptionTests {

    @Test
    public void testMetricStatePathExceptionString() {
        String pathName = "aaa.bbb";
        Exception e = new MetricStatePathException( pathName);

        String expectedValue = MetricStatePathException.DEFAULT_PREFIX + pathName;

        assertEquals( "MetricStatePathException msg with String constructor not as expected", expectedValue, e.toString());
    }

    @Test
    public void testMetricStatePathExceptionStatePathString() {
        String compositePathName = "aaa.bbb";
        String metricName = "metric";
        Exception e = new MetricStatePathException( StatePath.parsePath( compositePathName), metricName);

        String expectedValue = MetricStatePathException.DEFAULT_PREFIX + compositePathName + "." + metricName;

        assertEquals( "MetricStatePathException msg with StatePath, String constructor not as expected", expectedValue, e.toString());

    }

}
