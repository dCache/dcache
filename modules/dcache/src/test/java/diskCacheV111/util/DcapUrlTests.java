package diskCacheV111.util;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import diskCacheV111.util.DCapUrl;


public class DcapUrlTests {

    private static final String URI_SCHEME_DCAP = "dcap";
    private static final String URI_SCHEME_GSIDCAP = "gsidcap";
    private static final String URI_AUTHORITY = "dcap-door.example.org:22225";

    @Test
    public void testDcapUrl() {
        String path = "/pnfs/desy.de/h1/user/tigran/index.dat";

        DCapUrl url = buildDcapUrl( URI_SCHEME_DCAP, path, "");

        assertEquals("Invalid protocol", URI_SCHEME_DCAP, url.getProtocol() );
        assertEquals("Invalid protocol", path, url.getFilePart() );
    }

    @Test
    public void testDcapUrlWithOptions() {
        String path = "/pnfs/desy.de/h1/user/tigran/index.dat";
        String options = "filetype=raw";
        DCapUrl url = buildDcapUrl( URI_SCHEME_DCAP, path, options);

        assertEquals("Invalid protocol", URI_SCHEME_DCAP, url.getProtocol());
        assertEquals("Invalid protocol", path, url.getFilePart());
    }

    @Test
    public void testGsiDcapUrlWithOptions() {
        String path = "/pnfs/desy.de/h1/user/tigran/index.dat";
        DCapUrl url = buildDcapUrl( URI_SCHEME_GSIDCAP, path, "");

        assertEquals("Invalid protocol", URI_SCHEME_GSIDCAP, url.getProtocol());
        assertEquals("Invalid protocol", path, url.getFilePart());
    }

    @Test(expected=IllegalArgumentException.class)
    public void testRelativePathRejected() {
        new DCapUrl("relative-path/to/file");
    }

    @Test(expected=IllegalArgumentException.class)
    public void testRelativeUrlRejected() {
        new DCapUrl("/absolute/path/to/file");
    }

    @Test(expected=IllegalArgumentException.class)
    public void testUrlWithoutHostRejected() {
        new DCapUrl("dcap:/absolute/path/to/file");
    }

    @Test(expected=IllegalArgumentException.class)
    public void testUrlWithWrongSchemeRejected() {
        buildDcapUrl( "gsiftp", "/absolute/path/to/file", "");
    }

    @Test(expected=IllegalArgumentException.class)
    public void testUrlWithSlightlyWrongSchemeRejected() {
        buildDcapUrl( "dcaps", "/absolute/path/to/file", "");
    }

    @Test(expected=IllegalArgumentException.class)
    public void testOpaqueUrlRejected() {
        new DCapUrl("dcap:opaque-reference");
    }

    @Test(expected=IllegalArgumentException.class)
    public void testInvalidUri() {
        new DCapUrl("%");
    }


    /*
     * Support methods
     */

    private DCapUrl buildDcapUrl( String scheme, String path, String options) {
        String base = scheme + "://" + URI_AUTHORITY + path;
        String uri = base + (options.length() > 0 ? "?" + options : "");
        return new DCapUrl( uri);
    }
}
