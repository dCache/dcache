package org.dcache.gplazma.loader.cli;

import static org.junit.Assert.fail;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;

import org.dcache.gplazma.loader.PluginRepositoryFactory;
import org.dcache.gplazma.loader.StaticClassPluginRepositoryFactory;
import org.dcache.gplazma.plugins.GPlazmaPlugin;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableList;

public class ListCommandTests {
    private static final PluginRepositoryFactory FACTORY =
            new StaticClassPluginRepositoryFactory( ImmutableList.of( DummyPlugin.class,
                                                    AnotherDummyPlugin.class ));

    ListCommand _command;
    AnyOrderLineAsserter _asserter;

    @Before
    public void setUp() {
        _command = new ListCommand();
        _command.setFactory( FACTORY);

        OutputStream output = new ByteArrayOutputStream();
        _command.setOutput( new PrintStream(output));

        _asserter = new AnyOrderLineAsserter(output);
    }

    @Test
    public void testShortList() {
        _command.run(new String[0]);

        _asserter.add( "AnotherDummyPlugin (org.dcache.gplazma.loader.cli.ListCommandTests$AnotherDummyPlugin)");
        _asserter.add( "DummyPlugin (org.dcache.gplazma.loader.cli.ListCommandTests$DummyPlugin)");
        _asserter.run();
    }

    @Test
    public void testDetailedList() {
        _command.run(new String[]{"-l"});

        _asserter.add( "Plugin:");
        _asserter.add( "    Class: org.dcache.gplazma.loader.cli.ListCommandTests$AnotherDummyPlugin");
        _asserter.add( "    Name: AnotherDummyPlugin,org.dcache.gplazma.loader.cli.ListCommandTests$AnotherDummyPlugin");
        _asserter.add( "    Shortest name: AnotherDummyPlugin");
        _asserter.add( "Plugin:");
        _asserter.add( "    Class: org.dcache.gplazma.loader.cli.ListCommandTests$DummyPlugin");
        _asserter.add( "    Name: DummyPlugin,org.dcache.gplazma.loader.cli.ListCommandTests$DummyPlugin");
        _asserter.add( "    Shortest name: DummyPlugin");
        _asserter.run();
    }

    /**
     * Dummy implementation of a GPlazmaPlugin
     */
    public static final class DummyPlugin implements GPlazmaPlugin {
        // no content as the class isn't meant to be used.
    }

    /**
     * Another dummy implementation of a GPlazmaPlugin
     */
    public static final class AnotherDummyPlugin implements GPlazmaPlugin {
        // no content as the class isn't meant to be used.
    }


    /**
     * Verify that the expected lines all appear in the output and no other
     * lines appear, but be insensitive about the order in which the lines
     * appear.
     */
    public final class AnyOrderLineAsserter {
        private final List<String> _expectedLines = new ArrayList<String>();
        private final OutputStream _out;

        public AnyOrderLineAsserter( OutputStream out) {
            _out = out;
        }

        public void add(String line) {
            _expectedLines.add(line);
        }

        public void run() {
            List<String> notYetFoundLines = new ArrayList<String>(_expectedLines);
            String[] actualLines = _out.toString().split( "\n");

            int lineNumber=1;
            for( String actualLine : actualLines) {
                if( !notYetFoundLines.remove( actualLine)) {
                    fail( "[line " + lineNumber + "]" + " unexpected line in output: " + actualLine);
                }
                lineNumber++;
            }

            if( !notYetFoundLines.isEmpty()) {
                fail( "Missing output (" + notYetFoundLines.size() + ")");
            }
        }
    }
}
