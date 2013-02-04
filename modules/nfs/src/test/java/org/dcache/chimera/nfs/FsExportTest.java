package org.dcache.chimera.nfs;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

public class FsExportTest {

    private ExportFile _exportFile;

    @Before
    public void setUp() throws IOException {
        _exportFile = new ExportFile(ClassLoader.getSystemResource("org/dcache/chimera/nfs/exports"));
    }

    @Test
    public void testIsEmpty() {

        List<String> exports = _exportFile.getExports();
        assertFalse("Export file should not produce empty export list", exports.isEmpty());
    }

    @Test
    public void testIsLocalHostExplicit() throws UnknownHostException {

        FsExport export = _exportFile.getExport("/pnfs");
        InetAddress local = InetAddress.getByName("127.0.0.1");

        assertNotNull("null returned for existing export", export);

        assertTrue("localhost should always be allowed", export.isAllowed(local));

    }

    @Test
    public void testLocalAlwaysAllowed() throws UnknownHostException {

        FsExport export = _exportFile.getExport("/h1");
        InetAddress local = InetAddress.getByName("127.0.0.1");

        assertTrue("localhost should always be allowed", export.isAllowed(local));
    }

    @Test
    public void testMultimpleClients() {
        FsExport export = _exportFile.getExport("/h2");

        List<String> clients = export.client();

        assertEquals("Incorrect number on multiple allowed clients", 2, clients.size());
    }

    @Test
    public void testTrustedMultimpleClients() throws UnknownHostException {
        FsExport export = _exportFile.getExport("/trusted");

        InetAddress trusted = InetAddress.getByName("www.google.com");
        InetAddress nontrusted = InetAddress.getByName("www.yahoo.com");

        assertTrue("trusted host not respected", export.isTrusted(trusted) );
        assertFalse("nontrusted host respected", export.isTrusted(nontrusted) );
    }

    @Test
    public void testSubnets_B() throws UnknownHostException {
        FsExport export = _exportFile.getExport("/subnet_b");

        InetAddress allowed = InetAddress.getByName("192.168.2.2");
        InetAddress deny = InetAddress.getByName("192.168.3.1");

        assertTrue("Allowed host not recognized", export.isAllowed(allowed));
        assertFalse("Deny host not recognized", export.isAllowed(deny));
    }

    @Test
    public void testSubnets_C() throws UnknownHostException {
        FsExport export = _exportFile.getExport("/subnet_c");

        InetAddress allowed = InetAddress.getByName("192.168.2.2");
        InetAddress deny = InetAddress.getByName("192.169.2.2");

        assertTrue("Allowed host not recognized", export.isAllowed(allowed));
        assertFalse("Deny host not recognized", export.isAllowed(deny));
    }

    @Test
    public void testSubnets_Bad() throws UnknownHostException {
        FsExport export = _exportFile.getExport("/subnet_bad");

        InetAddress deny1 = InetAddress.getByName("192.168.2.1");
        InetAddress deny2 = InetAddress.getByName("192.169.2.2");

        assertFalse("Deny host not recognized", export.isAllowed(deny1));
        assertFalse("Deny host not recognized", export.isAllowed(deny2));
    }

    @Test
    public void testGetRootExport() throws UnknownHostException {
        assertNotNull(_exportFile.getExport("/"));
    }
}
