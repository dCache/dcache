package org.dcache.pool.classic;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@RunWith(Enclosed.class)
public class HotfileMonitoringTest {

    @RunWith(SpringJUnit4ClassRunner.class)
    @ContextConfiguration(locations = { "classpath:org/dcache/pool/classic/hotfile-monitoring-test.xml" })
    @TestPropertySource(properties = "pool.hotfile.monitoring.enable=true")
    public static class Enabled {
        @Autowired
        private HotfileMonitoringTestBean testBean;

        @Test
        public void testEnabled() {
            assertNotNull("FileRequestMonitor should be enabled", testBean.getFileRequestMonitor());
        }
    }

    @RunWith(SpringJUnit4ClassRunner.class)
    @ContextConfiguration(locations = { "classpath:org/dcache/pool/classic/hotfile-monitoring-test.xml" })
    @TestPropertySource(properties = "pool.hotfile.monitoring.enable=false")
    public static class Disabled {
        @Autowired
        private HotfileMonitoringTestBean testBean;

        @Test
        public void testDisabled() {
            assertNull("FileRequestMonitor should be disabled", testBean.getFileRequestMonitor());
        }
    }
}

class HotfileMonitoringTestBean {
    private Object fileRequestMonitor;
    public void setFileRequestMonitor(Object fileRequestMonitor) {
        this.fileRequestMonitor = fileRequestMonitor;
    }
    public Object getFileRequestMonitor() {
        return fileRequestMonitor;
    }
}
