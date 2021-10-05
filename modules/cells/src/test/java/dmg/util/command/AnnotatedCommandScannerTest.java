package dmg.util.command;

import static java.util.Arrays.asList;

import dmg.cells.nucleus.DelayedReply;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import org.dcache.util.Args;
import org.dcache.util.cli.AnnotatedCommandScanner;
import org.dcache.util.cli.CommandExecutor;
import org.junit.Before;
import org.junit.Test;

public class AnnotatedCommandScannerTest {

    private enum AnEnum {FOO, BAR}

    private AnnotatedCommandScanner _scanner;

    @Before
    public void setUp() {
        _scanner = new AnnotatedCommandScanner();
    }

    @Test
    public void shouldAllowReplyReturn() throws Exception {
        class SUT {

            @Command(name = "test")
            class TestCommand implements Callable<DelayedReply> {

                @Override
                public DelayedReply call() {
                    return new DelayedReply();
                }
            }
        }

        Map<List<String>, ? extends CommandExecutor> commands =
              _scanner.scan(new SUT());
        commands.get(asList("test")).execute(new Args(""));
    }
}
