package org.dcache.pool.json;

import org.junit.Test;
import java.io.PrintWriter;
import java.io.StringWriter;
import static org.junit.Assert.assertTrue;

public class PoolDataDetailsTest {

    @Test
    public void shouldPrintHotfileReplicationStatus() {
        PoolDataDetails details = new PoolDataDetails();
        details.setHotFileReplicationEnabled(true);

        StringWriter sw = new StringWriter();
        try (PrintWriter pw = new PrintWriter(sw)) {
            details.print(pw);
        }

        String output = sw.toString();
        assertTrue("Output should contain Hot File Replication status", output.contains("Hot File Replication : ON"));

        details.setHotFileReplicationEnabled(false);
        sw = new StringWriter();
        try (PrintWriter pw = new PrintWriter(sw)) {
            details.print(pw);
        }
        output = sw.toString();
        assertTrue("Output should contain Hot File Replication status", output.contains("Hot File Replication : OFF"));
    }
}
