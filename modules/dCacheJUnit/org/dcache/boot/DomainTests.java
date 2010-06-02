package org.dcache.boot;

import static org.junit.Assert.assertEquals;

import java.util.Properties;

import org.dcache.util.ReplaceableProperties;
import org.junit.Before;
import org.junit.Test;

public class DomainTests {

    private static final String DOMAIN_NAME = "domainName";

    // Domain always has a "domain.name" property who's value is the domain's name.
    private static final String PROPERTY_DOMAIN_NAME_KEY = "domain.name";
    private static final String PROPERTY_DOMAIN_NAME_VALUE = DOMAIN_NAME;

    Domain _domain;

    @Before
    public void setUp() {
        ReplaceableProperties properties = new ReplaceableProperties( new Properties());
        _domain = new Domain( DOMAIN_NAME, properties);
    }

    @Test
    public void testCreateWithNoGlobalProperties() {
        assertEquals(DOMAIN_NAME, _domain.getName());
        assertDomainServicesCount( 0);
        assertDomainPropertiesCount( 1);
        assertDomainPropertyValue( PROPERTY_DOMAIN_NAME_KEY, PROPERTY_DOMAIN_NAME_VALUE);
    }


    @Test
    public void testCreateWithAGlobalProperty() {
        String globalPropertyKey = "global.property";
        String globalPropertyValue = "global property value";
        ReplaceableProperties globalProperties = new ReplaceableProperties( new Properties());
        globalProperties.setProperty( globalPropertyKey, globalPropertyValue);

        _domain = new Domain( DOMAIN_NAME, globalProperties);

        assertEquals(DOMAIN_NAME, _domain.getName());
        assertDomainServicesCount( 0);


        // NB global properties are imported into Properties as "default" values, so
        // are not included in the property count.
        assertDomainPropertiesCount( 1);

        assertDomainPropertyValue( PROPERTY_DOMAIN_NAME_KEY, PROPERTY_DOMAIN_NAME_VALUE);
        assertDomainPropertyValue( globalPropertyKey, globalPropertyValue);
    }


    /*
     * SUPPORT METHODS
     */
    private void assertDomainServicesCount( int expectedCount) {
        assertEquals( expectedCount, _domain.getServices().size());
    }

    private void assertDomainPropertiesCount( int expectedCount) {
        ReplaceableProperties properties = _domain.properties();
        assertEquals(expectedCount, properties.size());
    }

    private void assertDomainPropertyValue( String key, String expectedValue) {
        assertEquals(expectedValue, _domain.properties().getReplacement(key));
    }
}
