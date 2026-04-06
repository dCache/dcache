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
        PrintWriter pw = new PrintWriter(sw);
        details.print(pw);

        String output = sw.toString();
        assertTrue("Output should contain Hotfile Repl. status", output.contains("Hotfile Repl.     : ON"));

        details.setHotFileReplicationEnabled(false);
        sw = new StringWriter();
        pw = new PrintWriter(sw);
        details.print(pw);
        output = sw.toString();
        assertTrue("Output should contain Hotfile Repl. status", output.contains("Hotfile Repl.     : OFF"));
    }
}
