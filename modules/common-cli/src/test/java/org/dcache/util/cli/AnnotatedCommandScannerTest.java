package org.dcache.util.cli;

import dmg.util.command.CommandLine;
import dmg.util.command.HelpFormat;
import dmg.util.command.Option;
import dmg.util.command.Argument;
import dmg.util.command.Command;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.sql.Time;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import dmg.util.CommandSyntaxException;
import org.dcache.util.Args;

import static java.util.Arrays.asList;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

public class AnnotatedCommandScannerTest
{
    private enum AnEnum { FOO, BAR }

    private AnnotatedCommandScanner _scanner;

    @Before
    public void setUp() throws Exception
    {
        _scanner = new AnnotatedCommandScanner();
    }

    @Test
    public void shouldUseCommandAnnotationToIdentifyCallableCommands() throws Exception
    {
        class SUT {
            @Command(name="test")
            class TestCommand implements Callable<String>
            {
                @Override
                public String call() throws Exception
                {
                    return null;
                }
            }
        }

        Map<List<String>,? extends CommandExecutor> commands =
                _scanner.scan(new SUT());

        assertThat(commands, hasKey(asList("test")));
    }

    @Test
    public void shouldIgnoreNonCallableCommands() throws Exception
    {
        class SUT {
            @Command(name="test")
            class TestCommand
            {
            }
        }

        Map<List<String>,? extends CommandExecutor> commands =
                _scanner.scan(new SUT());

        assertTrue(commands.isEmpty());
    }

    @Test
    public void shouldIgnoreUnannotatedCallables() throws Exception
    {
        class SUT {
            class TestCommand implements Callable<String>
            {
                @Override
                public String call() throws Exception
                {
                    return null;
                }
            }
        }

        Map<List<String>,? extends CommandExecutor> commands =
                _scanner.scan(new SUT());

        assertTrue(commands.isEmpty());
    }

    @Test
    public void shouldUseAclAnnotationForSingleAcl() throws Exception
    {
        class SUT {
            @Command(name="test", acl="acl")
            class TestCommand implements Callable<String>
            {
                @Override
                public String call() throws Exception
                {
                    return null;
                }
            }
        }

        Map<List<String>,? extends CommandExecutor> commands =
                _scanner.scan(new SUT());

        assertThat(commands.get(asList("test")).hasACLs(), is(true));
        assertThat(commands.get(asList("test"))
                .getACLs(), hasItemInArray("acl"));
    }

    @Test
    public void shouldUseAclAnnotationForMultibleAcls() throws Exception
    {
        class SUT {
            @Command(name="test", acl={"acl1", "acl2"})
            class TestCommand implements Callable<String>
            {
                @Override
                public String call() throws Exception
                {
                    return null;
                }
            }
        }

        Map<List<String>,? extends CommandExecutor> commands =
                _scanner.scan(new SUT());

        assertThat(commands.get(asList("test")).hasACLs(), is(true));
        assertThat(commands.get(asList("test")).getACLs(), hasItemInArray("acl1"));
        assertThat(commands.get(asList("test"))
                .getACLs(), hasItemInArray("acl2"));
    }

    @Test
    public void shouldUseHintAnnotationForHelpHint() throws Exception
    {
        class SUT {
            @Command(name="test", hint="hint")
            class TestCommand implements Callable<String>
            {
                @Override
                public String call() throws Exception
                {
                    return null;
                }
            }
        }

        Map<List<String>,? extends CommandExecutor> commands =
                _scanner.scan(new SUT());

        assertThat(commands.get(asList("test")).getHelpHint(HelpFormat.PLAIN), is("# hint"));
    }

    @Test
    public void shouldUseArgumentAnnotationForArguments() throws Exception
    {
        class SUT {
            @Command(name="test")
            class TestCommand implements Callable<String>
            {
                @Argument(index = 0)
                int argument1;

                @Argument(index = 1)
                String argument2;

                @Override
                public String call() throws Exception
                {
                    assertThat(argument1, is(1));
                    assertThat(argument2, is("2"));
                    return null;
                }
            }
        }

        Map<List<String>,? extends CommandExecutor> commands =
                _scanner.scan(new SUT());

        commands.get(asList("test")).execute(new Args("1 2"));
    }

    @Test
    public void shouldUseNegativeArgumentArgumentIndexToCountFromTheEnd() throws Exception
    {
        class SUT {
            @Command(name="test")
            class TestCommand implements Callable<String>
            {
                @Argument(index = -2)
                int argument;

                @Override
                public String call() throws Exception
                {
                    assertThat(argument, is(1));
                    return null;
                }
            }
        }

        Map<List<String>,? extends CommandExecutor> commands =
                _scanner.scan(new SUT());

        commands.get(asList("test")).execute(new Args("1 2"));
    }

    @Test
    public void shouldReturnResultOfCallable() throws Exception
    {
        class SUT {
            @Command(name="test")
            class TestCommand implements Callable<String>
            {
                @Override
                public String call() throws Exception
                {
                    return "result";
                }
            }
        }

        Map<List<String>,? extends CommandExecutor> commands =
                _scanner.scan(new SUT());

        assertThat(commands.get(asList("test")).execute(new Args("")),
                is((Object) "result"));
    }

    @Test(expected=CommandSyntaxException.class)
    public void shouldRejectTooFewArguments() throws Exception
    {
        class SUT {
            @Command(name="test")
            class TestCommand implements Callable<String>
            {
                @Argument
                int required;

                @Override
                public String call() throws Exception
                {
                    return null;
                }
            }
        }

        Map<List<String>,? extends CommandExecutor> commands =
                _scanner.scan(new SUT());

        commands.get(asList("test")).execute(new Args(""));
    }

    @Test(expected=CommandSyntaxException.class)
    public void shouldRejectTooManyArguments() throws Exception
    {
        class SUT {
            @Command(name="test")
            class TestCommand implements Callable<String>
            {
                @Argument
                int required;

                @Override
                public String call() throws Exception
                {
                    return null;
                }
            }
        }

        Map<List<String>,? extends CommandExecutor> commands =
                _scanner.scan(new SUT());

        commands.get(asList("test")).execute(new Args("1 2"));
    }

    @Test
    public void shouldAcceptOptionalArguments() throws Exception
    {
        class SUT {
            @Command(name="test")
            class TestCommand implements Callable<String>
            {
                @Argument(index=-2, required=false)
                Integer optional;

                @Argument(index=-1)
                Integer required;

                @Override
                public String call() throws Exception
                {
                    assertThat(optional, is(1));
                    assertThat(required, is(2));
                    return null;
                }
            }
        }

        Map<List<String>,? extends CommandExecutor> commands =
                _scanner.scan(new SUT());

        commands.get(asList("test")).execute(new Args("1 2"));
    }

    @Test
    public void shouldNotRequireOptionalArguments() throws Exception
    {
        class SUT {
            @Command(name="test")
            class TestCommand implements Callable<String>
            {
                @Argument(index=-2, required=false)
                Integer optional;

                @Argument(index=-1)
                Integer required;

                @Override
                public String call() throws Exception
                {
                    assertThat(optional, is((Integer) null));
                    assertThat(required, is(2));
                    return null;
                }
            }
        }

        Map<List<String>,? extends CommandExecutor> commands =
                _scanner.scan(new SUT());

        commands.get(asList("test")).execute(new Args("2"));
    }

    @Test
    public void shouldRespectDefaultValues() throws Exception
    {
        class SUT {
            @Command(name="test")
            class TestCommand implements Callable<String>
            {
                @Argument(index=1, required=false)
                int optional = 10;

                @Override
                public String call() throws Exception
                {
                    assertThat(optional, is(10));
                    return null;
                }
            }
        }

        Map<List<String>,? extends CommandExecutor> commands =
                _scanner.scan(new SUT());

        commands.get(asList("test")).execute(new Args(""));
    }

    @Test
    public void shouldAcceptCommonTypes() throws Exception
    {
        class SUT {
            @Command(name="test")
            class TestCommand implements Callable<String>
            {
                @Argument(index=0)
                Integer arg0;

                @Argument(index=1)
                int arg1;

                @Argument(index=2)
                Short arg2;

                @Argument(index=3)
                short arg3;

                @Argument(index=4)
                Long arg4;

                @Argument(index=5)
                long arg5;

                @Argument(index=6)
                Byte arg6;

                @Argument(index=7)
                byte arg7;

                @Argument(index=8)
                Float arg8;

                @Argument(index=9)
                float arg9;

                @Argument(index=10)
                Double arg10;

                @Argument(index=11)
                double arg11;

                @Argument(index=12)
                String arg12;

                @Argument(index=13)
                Character arg13;

                @Argument(index=14)
                char arg14;

                @Argument(index=15)
                AnEnum arg15;

                @Argument(index=16)
                File arg16;

                @Argument(index=17)
                Time arg17;

                @Argument(index=18)
                long[] arg18;

                @Override
                public String call() throws Exception
                {
                    assertThat(arg0, is(0));
                    assertThat(arg1, is(1));
                    assertThat(arg2, is((short) 2));
                    assertThat(arg3, is((short) 3));
                    assertThat(arg4, is(4L));
                    assertThat(arg5, is(5L));
                    assertThat(arg6, is((byte) 6));
                    assertThat(arg7, is((byte) 7));
                    assertThat(arg8, is((float) 8.0));
                    assertThat(arg9, is((float) 9.0));
                    assertThat(arg10, is(10.0));
                    assertThat(arg11, is(11.0));
                    assertThat(arg12, is("12"));
                    assertThat(arg13, is('a'));
                    assertThat(arg14, is('b'));
                    assertThat(arg15, is(AnEnum.BAR));
                    assertThat(arg16, is(new File("/my/file")));
                    assertThat(arg17, is(Time.valueOf("12:34:56")));
                    assertArrayEquals(arg18, new long[] { 100, 101 });
                    return null;
                }
            }
        }

        Map<List<String>,? extends CommandExecutor> commands =
                _scanner.scan(new SUT());

        commands.get(asList("test")).execute(new Args("0 1 2 3 4 5 6 7 8.0 9 10 11.00 12 a b BAR /my/file 12:34:56 100 101"));
    }

    @Test
    public void shouldOptionAnnotationForOptions() throws Exception
    {
        class SUT {
            @Command(name="test")
            class TestCommand implements Callable<String>
            {
                @Option(name="foo")
                boolean bar;


                @Override
                public String call() throws Exception
                {
                    assertThat(bar, is(true));
                    return null;
                }
            }
        }

        Map<List<String>,? extends CommandExecutor> commands =
                _scanner.scan(new SUT());

        commands.get(asList("test")).execute(new Args("-foo"));
    }

    @Test
    public void shouldAllowOptionArguments() throws Exception
    {
        class SUT {
            @Command(name="test")
            class TestCommand implements Callable<String>
            {
                @Option(name="foo")
                int bar;


                @Override
                public String call() throws Exception
                {
                    assertThat(bar, is(2));
                    return null;
                }
            }
        }

        Map<List<String>,? extends CommandExecutor> commands =
                _scanner.scan(new SUT());

        commands.get(asList("test")).execute(new Args("-foo=2"));
    }

    @Test(expected=CommandSyntaxException.class)
    public void shouldEnforceRequiredOptions() throws Exception
    {
        class SUT {
            @Command(name="test")
            class TestCommand implements Callable<String>
            {
                @Option(name="foo", required=true)
                int bar;


                @Override
                public String call() throws Exception
                {
                    assertThat(bar, is(2));
                    return null;
                }
            }
        }

        Map<List<String>,? extends CommandExecutor> commands =
                _scanner.scan(new SUT());

        commands.get(asList("test")).execute(new Args(""));
    }

    @Test
    public void shouldAllowOptionalOptions() throws Exception
    {
        class SUT {
            @Command(name="test")
            class TestCommand implements Callable<String>
            {
                @Option(name="foo")
                int bar;


                @Override
                public String call() throws Exception
                {
                    assertThat(bar, is(0));
                    return null;
                }
            }
        }

        Map<List<String>,? extends CommandExecutor> commands =
                _scanner.scan(new SUT());

        commands.get(asList("test")).execute(new Args(""));
    }

    @Test(expected = RuntimeException.class)
    public void shouldFailOnMissingConstructor() throws Exception
    {
        class SUT {
            @Command(name="test")
            class TestCommand implements Callable<String>
            {
                public TestCommand(int invalid)
                {
                }

                @Override
                public String call() throws Exception
                {
                    return null;
                }
            }
        }

        _scanner.scan(new SUT());
    }

    @Test(expected = RuntimeException.class)
    public void shouldFailOnNonSerializableReturnValue() throws Exception
    {
        class ReturnType
        {
        }

        class SUT
        {
            @Command(name = "test")
            class TestCommand implements Callable<ReturnType>
            {
                @Override
                public ReturnType call() throws Exception
                {
                    return null;
                }
            }
        }

        _scanner.scan(new SUT());
    }

    @Test
    public void shouldAcceptInheritedCommands() throws Exception
    {
        class SUT
        {
            class AbstractCommand implements Callable<String>
            {
                @Override
                public String call() throws Exception
                {
                    return null;
                }
            }

            @Command(name = "test")
            class TestCommand extends AbstractCommand
            {
            }
        }

        _scanner.scan(new SUT());
    }

    @Test
    public void shouldUseCommandOfSubClassWhenInjectingCommandLine() throws Exception
    {
        class SUT
        {
            @Command(name = "base")
            class BaseCommand implements Callable<String>
            {
                @CommandLine
                public String line;

                @Override
                public String call() throws Exception
                {
                    assertThat(line, is("base"));
                    return null;
                }
            }

            @Command(name = "test")
            class TestCommand extends BaseCommand
            {
                @Override
                public String call() throws Exception
                {
                    assertThat(line, is("test"));
                    return null;
                }
            }
        }

        Map<List<String>,? extends CommandExecutor> commands =
                _scanner.scan(new SUT());
        commands.get(asList("base")).execute(new Args(""));
        commands.get(asList("test")).execute(new Args(""));
    }
}
