package org.dcache.boot;

import org.junit.Before;
import org.junit.Test;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.LineNumberReader;
import java.io.PrintStream;
import java.io.StringReader;
import java.util.Properties;

import org.dcache.util.ConfigurationProperties;

import static org.dcache.boot.Properties.PROPERTY_CELL_NAME_SUFFIX;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;

/**
 *  Tests for the Python Oracle
 */
public class PythonOracleLayoutPrinterTests
{
    private ConfigurationProperties _globalDefaults;
    private ConfigurationProperties _dCacheConf;
    private ConfigurationProperties _layoutDefaults;
    private Layout _layout;
    private PythonOracleLayoutPrinter _printer;
    private ScriptEngine _engine;


    @Before
    public void setUp()
    {
        _globalDefaults = new ConfigurationProperties(new Properties());
        _globalDefaults.setProperty(org.dcache.boot.Properties.PROPERTY_DOMAIN_SERVICE_URI, "classpath:/org/dcache/boot/empty.batch");
        _dCacheConf = new ConfigurationProperties(_globalDefaults);

        _layout = new Layout(_dCacheConf);
        _layoutDefaults = _layout.properties();

        _printer = new PythonOracleLayoutPrinter(_layout);
        _engine = new ScriptEngineManager().getEngineByName("python");
    }

    @Test
    public void shouldFindGlobalValueIfDefined()
    {
        givenDefaults().with("property.name", "default value");

        whenOracleIsLoadedAndExec();

        assertThat(globalScopedProperty("property.name"), is("default value"));
    }

    @Test
    public void shouldFindAwkwardGlobalValueIfDefined()
    {
        givenDefaults().with("property.name", "the\\value\nis\t'here'");

        whenOracleIsLoadedAndExec();

        assertThat(globalScopedProperty("property.name"),
                is("the\\value\nis\t'here'"));
    }

    @Test
    public void shouldNotFindGlobalValueIfNotDefined()
    {
        givenDefaults().with("property.name", "default value");

        whenOracleIsLoadedAndExec();

        assertThat(globalScopedProperty("a.different.property.name"),
                is(nullValue()));
    }


    @Test
    public void shouldFindValueIfDefinedInDcacheConf()
    {
        givenDcacheConf().with("property.name", "dCache.conf value");

        whenOracleIsLoadedAndExec();

        assertThat(globalScopedProperty("property.name"),
                is("dCache.conf value"));
    }


    @Test
    public void shouldReturnValueIfDcacheConfOverridesDefault()
    {
        givenDefaults().with("property.name", "default value");
        givenDcacheConf().with("property.name", "dCache.conf value");

        whenOracleIsLoadedAndExec();

        assertThat(globalScopedProperty("property.name"),
                is("dCache.conf value"));
    }


    @Test
    public void shouldNotFindValueDefinedInDomain()
    {
        givenDomain("domain 1").with("property.name", "domain 1 value");

        whenOracleIsLoadedAndExec();

        assertThat(globalScopedProperty("property.name"), is(nullValue()));
    }


    @Test
    public void shouldFindValueDefinedInDomain()
    {
        givenDomain("domain 1").with("property.name", "domain 1 value");

        whenOracleIsLoadedAndExec();

        assertThat(domainScopedProperty("domain 1", "property.name"),
                is("domain 1 value"));
    }


    @Test
    public void shouldFindDomainValueIfGlobalDefined()
    {
        givenDefaults().with("property.name", " defaultvalue");
        givenDcacheConf().with("property.name", "dCache.conf value");
        givenDomain("domain 1").with("property.name", "domain 1 value");

        whenOracleIsLoadedAndExec();

        assertThat(domainScopedProperty("domain 1", "property.name"),
                is("domain 1 value"));
        assertThat(globalScopedProperty("property.name"),
                is("dCache.conf value"));
    }


    @Test
    public void shouldFindServiceValueIfDefined() throws IOException
    {
        givenDefaults().with("property.name", "default value");
        givenDcacheConf().with("property.name", "dCache.conf value");
        givenDomain("domain 1").
                with("property.name", "domain 1 value").
                withService("pool", "pool1").
                with("property.name", "pool1-value");

        whenOracleIsLoadedAndExec();

        assertThat(serviceScopedProperty("domain 1", "pool1", "property.name"),
                is("pool1-value"));
        assertThat(domainScopedProperty("domain 1", "property.name"),
                is("domain 1 value"));
        assertThat(globalScopedProperty("property.name"),
                is("dCache.conf value"));
    }

    @Test
    public void shouldFindSpecificServiceValueIfDefined() throws IOException
    {
        givenDefaults().with("property.name", "default value");
        givenDcacheConf().with("property.name", "dCache.conf value");
        givenDomain("domain 1").
                with("property.name", "domain 1 value").
                withService("pool", "pool1").
                with("property.name", "pool1 value").
                withService("pool", "pool2").
                with("property.name", "pool2 value");

        whenOracleIsLoadedAndExec();

        assertThat(serviceScopedProperty("domain 1", "pool1", "property.name"),
                is("pool1 value"));
        assertThat(serviceScopedProperty("domain 1", "pool2", "property.name"),
                is("pool2 value"));
    }

    private void whenOracleIsLoadedAndExec()
    {
        ByteArrayOutputStream stored = new ByteArrayOutputStream();
        PrintStream s = new PrintStream(stored);
        _printer.print(s);

        // We assign the variable 'declaration' with the Oracle definition.
        // This is to simulate running the bootloader to generate the python
        // oracle.
        _engine.put("declaration", stored.toString());

        try {
            _engine.eval("exec declaration");
        } catch (ScriptException e) {
            throw new RuntimeException(e);
        }
    }

    private String globalScopedProperty(String key)
    {
        String args = "'" + key + "'";
        return lookupArgs(args);
    }

    private String domainScopedProperty(String domain, String key)
    {
        String args = "'" + key +"', '" + domain + "'";
        return lookupArgs(args);
    }

    private String serviceScopedProperty(String domain, String service, String key)
    {
        String args = "'" + key +"', '" + domain + "', '" + service + "'";
        return lookupArgs(args);
    }

    private String lookupArgs(String args)
    {
        try {
            _engine.eval("result = properties.get(" + args + ")");
        } catch (ScriptException e) {
            throw new RuntimeException(e);
        }

        Object result = _engine.get("result");
        return result == null ? null : result.toString();
    }

    private PropertyBuilder givenDefaults()
    {
        return new PropertyBuilder(_globalDefaults);
    }

    private PropertyBuilder givenDcacheConf()
    {
        return new PropertyBuilder(_dCacheConf);
    }

    private PropertyBuilder givenLayoutDefaults()
    {
        return new PropertyBuilder(_layoutDefaults);
    }

    /**
     * Class with fluent interface that allows initialising a
     * ConfigurationProperties.
     */
    private class PropertyBuilder
    {
        private final ConfigurationProperties _inner;

        public PropertyBuilder(ConfigurationProperties properties)
        {
            _inner = properties;
        }

        public PropertyBuilder with(String key, String value)
        {
            _inner.put(key, value);
            return this;
        }
    }

    private DomainBuilder givenDomain(String name)
    {
        return new DomainBuilder(name);
    }


    /**
     * Class with fluent interface that allows building of a domain
     * with domain-scoped default values and services with service-scoped
     * property values.
     */
    private class DomainBuilder
    {
        private final Domain _domain;
        private ConfigurationProperties _properties;

        public DomainBuilder(String name)
        {
            _layout.createDomain(name);
            _domain = _layout.getDomain(name);
            _properties = _domain.properties();
        }

        public DomainBuilder with(String key, String value)
        {
            _properties.put(key, value);
            return this;
        }

        public DomainBuilder withService(String type, String cellName) throws IOException
        {
            _properties = _domain.createService("source", new LineNumberReader(new StringReader("")), type);
            _properties.put(type + "." + PROPERTY_CELL_NAME_SUFFIX, cellName);
            return this;
        }
    }

}
