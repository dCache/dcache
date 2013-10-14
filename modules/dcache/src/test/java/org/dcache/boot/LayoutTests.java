package org.dcache.boot;

import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.StringReader;
import java.util.List;
import java.util.Properties;

import org.dcache.util.ConfigurationProperties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class LayoutTests {

    private static final String PROPERTY_DOMAIN_NAME_KEY = "dcache.domain.name";

    Layout _layout;
    LayoutStringBuffer _readerSource;

    @Before
    public void setUp() {
        ConfigurationProperties config = new ConfigurationProperties(new Properties());
        config.setProperty(org.dcache.boot.Properties.PROPERTY_DOMAIN_SERVICE_URI, "classpath:/org/dcache/boot/empty.batch");
        _layout = new Layout(config);

        _readerSource = new LayoutStringBuffer();
    }

    @Test
    public void testLoadSingleDomain() throws IOException {
        String domainName = "domainName";

        _readerSource.appendDomain( domainName);
        load();

        Domain domain = _layout.getDomain(domainName);
        assertNotNull(domain);
        assertEquals(domainName, domain.getName());
        assertDomainHasProperty( domain, PROPERTY_DOMAIN_NAME_KEY, domainName);
    }

    @Test
    public void testLoadSingleDomainWithProperty() throws IOException {
        String domainName = "domainName";
        String propertyName = "foo";
        String propertyValue = "bar";

        _readerSource.appendDomain( domainName);
        _readerSource.addProperty( propertyName, propertyValue);

        load();

        Domain domain = _layout.getDomain(domainName);
        assertNotNull(domain);
        assertDomainHasProperty( domain, PROPERTY_DOMAIN_NAME_KEY, domainName);
        assertDomainHasProperty( domain, propertyName, propertyValue);
    }

    @Test
    public void testLoadSingleDomainWithWhiteSpaceProperty() throws IOException {
        String domainName = "domainName";
        String propertyName = "foo";
        String propertyValue = "bar ";

        _readerSource.appendDomain(domainName);
        _readerSource.addProperty(propertyName, propertyValue);

        load();

        Domain domain = _layout.getDomain(domainName);
        assertNotNull(domain);
        assertDomainHasProperty(domain, PROPERTY_DOMAIN_NAME_KEY, domainName);
        assertDomainHasProperty(domain, propertyName, propertyValue.trim());
    }

    @Test
    public void testLoadSingleDomainWithGlobalProperty() throws IOException {
        String domainName = "domainName";
        String propertyName = "foo";
        String propertyValue = "bar";

        _readerSource.addProperty(propertyName, propertyValue);
        _readerSource.appendDomain(domainName);
        load();

        Domain domain = _layout.getDomain(domainName);
        assertNotNull(domain);
        assertDomainHasProperty( domain, PROPERTY_DOMAIN_NAME_KEY, domainName);
        assertDomainHasProperty( domain, propertyName, propertyValue);
    }

    @Test
    public void testLoadSingleDomainWithService() throws IOException {
        String domainName = "domainName";
        String serviceName = "serviceName";

        _readerSource.appendDomain( domainName);
        _readerSource.appendService( domainName, serviceName);
        load();

        Domain domain = _layout.getDomain(domainName);
        assertNotNull(domain);
        assertDomainHasProperty( domain, PROPERTY_DOMAIN_NAME_KEY, domainName);

        assertDomainServicesSize( domain, 1);

        ConfigurationProperties serviceProperties = domain.getServices().get(0);
        assertServicePropertySize( serviceProperties, 2);
        assertServiceHasProperty( serviceProperties, PROPERTY_DOMAIN_NAME_KEY, domainName);
    }


    @Test
    public void testSimpleLoadWithLeadingSpace() throws IOException {
        String domainName = "domainName";

        _readerSource.append(" ");
        _readerSource.appendDomain(domainName);
        load();

        Domain domain = _layout.getDomain(domainName);
        assertNotNull(domain);
    }

    /*
     * SUPPORT METHODS
     */

    private void assertDomainHasProperty( Domain domain, String propertyKey, String expectedValue) {
        Properties properties = domain.properties();
        assertEquals( expectedValue, properties.getProperty( propertyKey));
    }

    private void assertDomainServicesSize( Domain domain, int expectedSize) {
        List<ConfigurationProperties> services = domain.getServices();
        assertEquals( expectedSize, services.size());
    }

    private void assertServiceHasProperty( ConfigurationProperties properties, String propertyKey, String expectedValue) {
        assertEquals( expectedValue, properties.getProperty( propertyKey));
    }

    private void assertServicePropertySize( ConfigurationProperties properties, int expectedSize) {
        assertEquals(expectedSize,properties.size());
    }


    private void load() throws IOException {
        StringReader reader = new StringReader(_readerSource.toString());
        _layout.load(reader);
    }

    class LayoutStringBuffer {
        final private StringBuffer _sb = new StringBuffer();

        public LayoutStringBuffer append( String string) {
            _sb.append(string);
            return this;
        }

        public LayoutStringBuffer appendLine( String line) {
            _sb.append(line).append("\n");
            return this;
        }

        public LayoutStringBuffer appendDomain( String domainName) {
            appendLine( "[" + domainName + "]");
            return this;
        }

        public LayoutStringBuffer appendService( String domainName, String serviceName) {
            appendLine( "[" + domainName + "/" + serviceName + "]");
            return this;
        }

        public LayoutStringBuffer addProperty( String key, String value) {
            appendLine( key + "=" + value);
            return this;
        }

        @Override
        public String toString() {
            return _sb.toString();
        }
    }
}
