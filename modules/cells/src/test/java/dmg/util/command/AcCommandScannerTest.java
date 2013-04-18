package dmg.util.command;

import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import dmg.util.Args;
import dmg.util.CommandInterpreter;
import dmg.util.CommandRequestable;
import dmg.util.CommandSyntaxException;

import static dmg.util.command.HelpFormat.PLAIN;
import static java.util.Arrays.asList;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class AcCommandScannerTest
{
    private AcCommandScanner _scanner;

    @Before
    public void setUp() throws Exception
    {
        _scanner = new AcCommandScanner();
    }

    @Test
    public void shouldFindSingleAclField() throws Exception
    {
        Map<List<String>,? extends CommandExecutor> commands =
                _scanner.scan(new Object() {
                    public final String acl_test = "acl";
                });

        assertThat(commands.get(asList("test")).hasACLs(), is(true));
        assertThat(commands.get(asList("test")).getACLs(), hasItemInArray("acl"));
    }

    @Test
    public void shouldFindMultiAclField() throws Exception
    {
        Map<List<String>,? extends CommandExecutor> commands =
                _scanner.scan(new Object() {
                    public final String[] acl_test = { "acl", "acl2" };
                });

        assertThat(commands.get(asList("test")).hasACLs(), is(true));
        assertThat(commands.get(asList("test")).getACLs(), hasItemInArray("acl"));
        assertThat(commands.get(asList("test")).getACLs(), hasItemInArray("acl2"));
    }

    @Test
    public void shouldFindHintField() throws Exception
    {
        Map<List<String>,? extends CommandExecutor> commands =
                _scanner.scan(new Object() {
                    public static final String hh_test = "help";
                });

        assertThat(commands.get(asList("test")).getHelpHint(PLAIN), is("help"));
    }

    @Test
    public void shouldFindHelpField() throws Exception
    {
        Map<List<String>,? extends CommandExecutor> commands =
                _scanner.scan(new Object() {
                    public static final String fh_test = "long help";
                });

        assertThat((String) commands.get(asList("test")).getFullHelp(PLAIN), is("long help"));
    }

    @Test
    public void shouldFindStaticFields() throws Exception
    {
        Map<List<String>,? extends CommandExecutor> commands =
                _scanner.scan(new Object() {
                    public static final String fh_test = "long help";
                });

        assertThat((String) commands.get(asList("test")).getFullHelp(PLAIN), is("long help"));
    }

    @Test
    public void shouldFindCommandMethods() throws Exception
    {
        Map<List<String>,? extends CommandExecutor> commands =
                _scanner.scan(new Object() {
                    public Object ac_test(Args args) { return null; }
                });

        assertThat(commands, hasKey(asList("test")));
    }

    @Test(expected=CommandSyntaxException.class)
    public void shouldConsiderAbsentArityAsZero() throws Exception
    {
        Map<List<String>,? extends CommandExecutor> commands =
                _scanner.scan(new Object() {
                    public Object ac_test(Args args) {
                        return null;
                    }
                });

        commands.get(asList("test")).execute(new Args("a"), CommandInterpreter.ASCII);
    }

    @Test(expected=CommandSyntaxException.class)
    public void shouldConsiderSingleArityValueAsMax() throws Exception
    {
        Map<List<String>,? extends CommandExecutor> commands =
                _scanner.scan(new Object() {
                    public Object ac_test_$_1(Args args) {
                        return null;
                    }
                });

        commands.get(asList("test")).execute(new Args("a b"), CommandInterpreter.ASCII);
    }

    @Test(expected=CommandSyntaxException.class)
    public void shouldConsiderSingleArityValueAsMin() throws Exception
    {
        Map<List<String>,? extends CommandExecutor> commands =
                _scanner.scan(new Object() {
                    public Object ac_test_$_1(Args args) {
                        return null;
                    }
                });

        commands.get(asList("test")).execute(new Args(""), CommandInterpreter.ASCII);
    }

    @Test
    public void shouldConsiderSingleArityValueAsArgumentCount() throws Exception
    {
        Map<List<String>,? extends CommandExecutor> commands =
                _scanner.scan(new Object() {
                    public Object ac_test_$_1(Args args) {
                        return null;
                    }
                });

        commands.get(asList("test")).execute(new Args("a"), CommandInterpreter.ASCII);
    }

    @Test
    public void shouldConsiderTwoArityValuesAsRange() throws Exception
    {
        Map<List<String>,? extends CommandExecutor> commands =
                _scanner.scan(new Object() {
                    public Object ac_test_$_1_2(Args args) {
                        return null;
                    }
                });

        commands.get(asList("test")).execute(new Args("a"), CommandInterpreter.ASCII);
        commands.get(asList("test")).execute(new Args("a b"), CommandInterpreter.ASCII);
    }

    @Test(expected=CommandSyntaxException.class)
    public void shouldConsiderTwoArityValuesAsRangeWithLowerBound() throws Exception
    {
        Map<List<String>,? extends CommandExecutor> commands =
                _scanner.scan(new Object() {
                    public Object ac_test_$_1_2(Args args) {
                        return null;
                    }
                });

        commands.get(asList("test")).execute(new Args(""), CommandInterpreter.ASCII);
    }

    @Test(expected=CommandSyntaxException.class)
    public void shouldConsiderTwoArityValuesAsRangeWithUpperBound() throws Exception
    {
        Map<List<String>,? extends CommandExecutor> commands =
                _scanner.scan(new Object() {
                    public Object ac_test_$_1_2(Args args) {
                        return null;
                    }
                });

        commands.get(asList("test")).execute(new Args("a b c"), CommandInterpreter.ASCII);
    }

    @Test
    public void shouldPassArgumentsAlongToCommand() throws Exception
    {
        Map<List<String>,? extends CommandExecutor> commands =
                _scanner.scan(new Object() {
                    public Object ac_test_$_2(Args args) {
                        assertTrue(args.hasOption("o"));
                        assertThat(args.argc(), is(2));
                        assertThat(args.argv(0), is("a"));
                        assertThat(args.argv(1), is("b"));
                        return null;
                    }
                });

        commands.get(asList("test")).execute(new Args("-o a b"), CommandInterpreter.ASCII);
    }

    @Test
    public void shouldUseUnderscoreAsAWordSeparator() throws Exception
    {
        Map<List<String>,? extends CommandExecutor> commands =
                _scanner.scan(new Object() {
                    public Object ac_test1_test2(Args args) {
                        return null;
                    }
                });

        assertThat(commands, hasKey(asList("test1", "test2")));
    }

    @Test
    public void shouldAllowBothAsciiAndBinaryMethods() throws Exception
    {
        Map<List<String>,? extends CommandExecutor> commands =
            _scanner.scan(new Object() {
                public Object ac_test(Args args) {
                    return null;
                }

                public Object ac_test(CommandRequestable request) {
                    return null;
                }
            });

        commands.get(asList("test")).execute(new Args(""), CommandInterpreter.ASCII);
        commands.get(asList("test")).execute(new DummyCommandRequestable(), CommandInterpreter.BINARY);
    }

    private class DummyCommandRequestable implements CommandRequestable
    {
        @Override
        public String getRequestCommand()
        {
            return null;
        }

        @Override
        public int getArgc()
        {
            return 0;
        }

        @Override
        public Object getArgv(int position)
        {
            return null;
        }
    }
}
