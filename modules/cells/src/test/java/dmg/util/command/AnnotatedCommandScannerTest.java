package dmg.util.command;

import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import dmg.cells.nucleus.DelayedReply;

import org.dcache.util.Args;
import org.dcache.util.cli.AnnotatedCommandScanner;
import org.dcache.util.cli.CommandExecutor;

import static java.util.Arrays.asList;

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
    public void shouldAllowReplyReturn() throws Exception
    {
        class SUT
        {
            @Command(name = "test")
            class TestCommand implements Callable<DelayedReply>
            {
                @Override
                public DelayedReply call() throws Exception
                {
                    return new DelayedReply();
                }
            }
        }

        Map<List<String>, ? extends CommandExecutor> commands =
            _scanner.scan(new SUT());
        commands.get(asList("test")).execute(new Args(""));
    }
}
