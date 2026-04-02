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
    @ContextConfiguration(locations = {
          "classpath:org/dcache/pool/classic/hotfile-monitoring-test.xml"})
    @TestPropertySource(properties = "pool.hotfile.monitoring.enable=true")
    public static class Enabled {

        @Autowired
        private HotfileMonitoringTestBean testBean;

        @Test
        public void testEnabled() {
            assertNotNull("HotFileReplicator should be enabled", testBean.getHotFileReplicator());
        }
    }

    @RunWith(SpringJUnit4ClassRunner.class)
    @ContextConfiguration(locations = {
          "classpath:org/dcache/pool/classic/hotfile-monitoring-test.xml"})
    @TestPropertySource(properties = "pool.hotfile.monitoring.enable=false")
    public static class Disabled {

        @Autowired
        private HotfileMonitoringTestBean testBean;

        @Test
        public void testDisabled() {
            assertNull("HotFileReplicator should be disabled", testBean.getHotFileReplicator());
        }
    }
}

class HotfileMonitoringTestBean {

    private Object hotFileReplicator;

    public void setHotFileReplicator(Object hotFileReplicator) {
        this.hotFileReplicator = hotFileReplicator;
    }

    public Object getHotFileReplicator() {
        return hotFileReplicator;
    }
}
