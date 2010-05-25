package org.dcache.gplazma.loader;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;

import org.dcache.gplazma.plugins.GPlazmaPlugin;
import org.junit.Before;
import org.junit.Test;

public class StaticClassPluginLoaderTests {

    private static final String PLUGIN_NAME_VALID_PLUGIN = "ValidPlugin";
    private static final String PLUGIN_NAME_INVALID_PLUGIN = "InvalidPlugin";
    private static final String PLUGIN_NAME_EXCEPTION_PLUGIN = "ExceptionThrowingPlugin";

    /** Name of some non-existent plugin */
    private static final String PLUGIN_NAME_MISSING_PLUGIN = "NoSuchPlugin";

    private static final String[] ARGUMENTS = new String[]{"arg-1", "arg-2"};

    PluginLoader _loaderNotInit;
    PluginLoader _loader;

    @Before
    public void setUp() {
        _loaderNotInit = newLoader();
        _loader = newLoader();
        _loader.init();
    }

    @SuppressWarnings("unchecked") // a known issue with Generics and varargs
    public PluginLoader newLoader() {
        return StaticClassPluginLoader.newPluginLoader( ValidPlugin.class,
                                            InvalidPlugin.class,
                                            ExceptionThrowingPlugin.class);

    }

    @Test(expected=IllegalStateException.class)
    public void testNewPluginWithoutInit() {
        _loaderNotInit.newPluginByName(PLUGIN_NAME_VALID_PLUGIN);
    }

    @Test(expected=IllegalStateException.class)
    public void testDoubleInitFails() {
        _loaderNotInit.init();
        _loaderNotInit.init();
    }

    @Test(expected=IllegalArgumentException.class)
    public void testNewPluginWithWrongName() {
        _loader.newPluginByName(PLUGIN_NAME_MISSING_PLUGIN);
    }

    @Test(expected=IllegalArgumentException.class)
    public void testNewPluginWithInvalidPlugin() {
        _loader.newPluginByName(PLUGIN_NAME_INVALID_PLUGIN);
    }

    @Test
    public void testNewPluginByNameWithoutArgs() {
        GPlazmaPlugin plugin = _loader.newPluginByName(PLUGIN_NAME_VALID_PLUGIN);
        assertNotNull("plugin is real instance", plugin);
    }

    @Test
    public void testNewPluginByNameTwice() {
        GPlazmaPlugin plugin1 = _loader.newPluginByName(PLUGIN_NAME_VALID_PLUGIN);
        GPlazmaPlugin plugin2 = _loader.newPluginByName(PLUGIN_NAME_VALID_PLUGIN);
        assertNotNull("plugin1 is real instance", plugin1);
        assertNotNull("plugin2 is real instance", plugin2);
        assertNotSame("newPluginByName called twice is the same plugin", plugin1, plugin2);
    }

    @Test
    public void testNewPluginWithArgs() {
        GPlazmaPlugin genericPlugin = _loader.newPluginByName(PLUGIN_NAME_VALID_PLUGIN, ARGUMENTS);
        ValidPlugin plugin = (ValidPlugin) genericPlugin;
        assertArrayEquals("plugin has same array", ARGUMENTS, plugin.getArgs());
    }

    @Test(expected=IllegalArgumentException.class)
    public void testNewPluginByNameExceptionNoArgs() {
        _loader.newPluginByName(PLUGIN_NAME_EXCEPTION_PLUGIN);
    }


    @Test(expected=IllegalArgumentException.class)
    public void testNewPluginByNameExceptionWrongArgs() {
        String[] args = new String[]{"arg-1"};
        _loader.newPluginByName(PLUGIN_NAME_EXCEPTION_PLUGIN, args);
    }

    @Test
    public void testNewPluginByNameExceptionCorrectArgs() {
        String[] args = new String[]{ExceptionThrowingPlugin.REQUIRED_ARGUMENT};
        _loader.newPluginByName(PLUGIN_NAME_EXCEPTION_PLUGIN, args);
    }


    // An invalid plugin: it's missing the String[] args constructor
    public static class InvalidPlugin implements GPlazmaPlugin {
    }

    // An valid plugin that throws an exception is a required argument isn't specified
    public static class ExceptionThrowingPlugin implements GPlazmaPlugin {
        public static final String REQUIRED_ARGUMENT = "foo";

        public ExceptionThrowingPlugin( String arguments[]) {
            if( !haveValidArguments(arguments)) {
                throw new IllegalArgumentException( "Required argument not present");
            }
        }

        private boolean haveValidArguments( String args[]) {
            boolean requiredArgFound = false;

            for( String arg : args) {
                if( arg.equals( REQUIRED_ARGUMENT)) {
                    requiredArgFound = true;
                }
            }

            return requiredArgFound;
        }
    }

    // A simple plugin that allows inspection of supplied arguments
    public static class ValidPlugin implements GPlazmaPlugin {
        private String _args[];

        public ValidPlugin( String args[]) {
            _args = args;
        }

        public String[] getArgs() {
            return _args;
        }
    }
}
