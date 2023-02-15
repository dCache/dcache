package org.dcache.util;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsMapContaining.hasEntry;
import static org.hamcrest.collection.IsMapWithSize.aMapWithSize;
import static org.hamcrest.collection.IsMapWithSize.anEmptyMap;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import org.junit.Test;

public class XattrsTest {

    @Test
    public void testUriXattrEmpty() {
        URI uri = URI.create("http://server.example.com/foo");

        Map<String, String> xattrs = Xattrs.from(uri);
        assertThat(xattrs, anEmptyMap());
    }

    @Test
    public void testUriXattrSingleValue() {
        URI uri = URI.create("http://server.example.com/foo?xattr.key1=value1");

        Map<String, String> xattrs = Xattrs.from(uri);
        assertThat(xattrs, aMapWithSize(1));
        assertThat(xattrs, hasEntry("key1", "value1"));
    }

    @Test
    public void testUriXattrMutipleValue() {
        URI uri = URI.create("http://server.example.com/foo?xattr.key1=value1&xattr.key2=value2");

        Map<String, String> xattrs = Xattrs.from(uri);
        assertThat(xattrs, aMapWithSize(2));
        assertThat(xattrs, hasEntry("key1", "value1"));
        assertThat(xattrs, hasEntry("key2", "value2"));
    }

    @Test
    public void testUriXattrIgnoreOthers() {
        URI uri = URI.create("http://server.example.com/foo?xattr.key1=value1&key2=value2");

        Map<String, String> xattrs = Xattrs.from(uri);
        assertThat(xattrs, aMapWithSize(1));
        assertThat(xattrs, hasEntry("key1", "value1"));
    }

    @Test
    public void testUriXattrPercentEncodedSpaceKey() {
        URI uri = URI.create("http://server.example.com/foo?xattr.key%20name=value");

        Map<String, String> xattrs = Xattrs.from(uri);

        assertThat(xattrs, aMapWithSize(1));
        assertThat(xattrs, hasEntry("key name", "value"));
    }

    @Test
    public void testUriXattrEncodedPlusKey() {
        URI uri = URI.create("http://server.example.com/foo?xattr.key%2bname=value");

        Map<String, String> xattrs = Xattrs.from(uri);

        assertThat(xattrs, aMapWithSize(1));
        assertThat(xattrs, hasEntry("key+name", "value"));
    }

    @Test(expected = NullPointerException.class)
    public void testUriXattrNullArg() {
        Xattrs.from((URI) null);
    }

    @Test
    public void testMapXattrEmpty() {
        Map<String, String[]> params = new HashMap<>();

        Map<String, String> xattrs = Xattrs.from(params);
        assertThat(xattrs, anEmptyMap());
    }

    @Test
    public void testMapXattrSingleValue() {
        Map<String, String[]> params = Map.of(
              "xattr.key1", new String[]{"value1"}
        );

        Map<String, String> xattrs = Xattrs.from(params);
        assertThat(xattrs, aMapWithSize(1));
        assertThat(xattrs, hasEntry("key1", "value1"));
    }

    @Test
    public void testMapXattrMutipleValue() {
        Map<String, String[]> params = Map.of(
              "xattr.key1", new String[] {"value1"},
              "xattr.key2", new String[] {"value2"}
        );

        Map<String, String> xattrs = Xattrs.from(params);
        assertThat(xattrs, aMapWithSize(2));
        assertThat(xattrs, hasEntry("key1", "value1"));
        assertThat(xattrs, hasEntry("key2", "value2"));
    }

    @Test
    public void testMapXattrIgnoreOthers() {
        Map<String, String[]> params = Map.of(
              "xattr.key1", new String[] {"value1"},
              "key2", new String[] {"value2"}
        );

        Map<String, String> xattrs = Xattrs.from(params);
        assertThat(xattrs, aMapWithSize(1));
        assertThat(xattrs, hasEntry("key1", "value1"));
    }

    @Test
    public void testMapXattrFirstValueWins() {
        Map<String, String[]> params = Map.of(
              "xattr.key1", new String[] {"value1", "value2"}
        );

        Map<String, String> xattrs = Xattrs.from(params);
        assertThat(xattrs, aMapWithSize(1));
        assertThat(xattrs, hasEntry("key1", "value1"));
    }

    @Test(expected = NullPointerException.class)
    public void testMapXattrNullArg() {
        Xattrs.from((Map<String, String[]>) null);
    }
}
