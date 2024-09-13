package org.dcache.pool.repository.v5;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.dcache.pool.FaultAction;
import org.dcache.pool.repository.Account;
import org.dcache.pool.repository.FileStoreState;
import org.dcache.pool.repository.ReplicaStore;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

public class CheckHealthTaskTest {

    private final static String PROPERTY_PREFIX = "-";
    private final static String PROPERTY_KEY_VALUE_SEPARATOR = "=";
    private final static String PROPERTY_SEPARATOR = " ";

    private final static String PROPERTY_INT_KEY = "http-mover-client-idle-timeout";
    private final static String PROPERTY_INT_VALUE = "300";
    private final static int PROPERTY_INT_EXPECTED = 300;

    private final static String PROPERTY_LONG_KEY = "http-mover-connection-max-memory";
    private final static String PROPERTY_LONG_VALUE = "4294967297";
    private final static long PROPERTY_LONG_EXPECTED = 4294967297L;

    private final static String PROPERTY_DOUBLE_KEY = "fault-tolerance";
    private final static String PROPERTY_DOUBLE_VALUE = "0.00417";
    private final static double PROPERTY_DOUBLE_EXPECTED = 0.00417;

    private final static String PROPERTY_STRING_KEY = "xrootd-authn-plugin";
    private final static String PROPERTY_STRING_VALUE = "gsi";
    private final static String PROPERTY_STRING_EXPECTED = PROPERTY_STRING_VALUE;

    @Test
    public void testEmptyCommand() {
        String[] command = new CheckHealthTask.Scanner("").scan();
        assertEquals(0, command.length);
    }

    @Test
    public void testWithNoArgs() {
        String[] command = new CheckHealthTask.Scanner("command").scan();
        assertArrayEquals(new String[]{"command"}, command);
    }


    @Test
    public void testWithArgs() {
        String[] command = new CheckHealthTask.Scanner("command arg1 arg2 arg3 arg4 arg5").scan();
        assertArrayEquals(new String[]{"command", "arg1", "arg2", "arg3", "arg4", "arg5"}, command);
    }

    @Test
    public void testDoubleQuoteArgument() {
        String[] command = new CheckHealthTask.Scanner("\"foo bar\" bla").scan();
        assertArrayEquals(new String[]{"foo bar", "bla"}, command);
    }

    @Test
    public void testDoubleQuoteArgumentWithEscape() {
        String[] command = new CheckHealthTask.Scanner("foo \"b\\\"a\\\"r\"").scan();
        assertArrayEquals(new String[]{"foo", "b\"a\"r"}, command);
    }

    @Test
    public void testDoubleQuoteInsideArgument() {
        String[] command = new CheckHealthTask.Scanner("foo b\"a\"r").scan();
        assertArrayEquals(new String[]{"foo", "bar"}, command);
    }

    @Test
    public void testSingleQuoteArgument() {
        String[] command = new CheckHealthTask.Scanner("foo 'bar bla'").scan();
        assertArrayEquals(new String[]{"foo", "bar bla"}, command);
    }

    @Test
    public void testEscapedSpaceArgument() {
        String[] command = new CheckHealthTask.Scanner("bar\\ bar").scan();
        assertArrayEquals(new String[]{"bar bar"}, command);
    }

    @Test
    public void testEscapedBackslash() {
        String[] command = new CheckHealthTask.Scanner("bar\\\\bar").scan();
        assertArrayEquals(new String[]{"bar\\bar"}, command);
    }

    @Test
    public void testNoRepositoryCheckOnCommand() {

        var repository = Mockito.mock(ReplicaRepository.class);
        var replicaStore = Mockito.mock(ReplicaStore.class);
        var account = new Account();
        when(repository.getState()).thenReturn(ReplicaRepository.State.OPEN);

        var check = spy(new CheckHealthTask());
        check.setAccount(account);
        check.setReplicaStore(replicaStore);
        check.setRepository(repository);
        check.setCommand("/bin/true");

        check.run();

        verify(replicaStore, never()).isOk();
        verify(check).checkHealthCommand();
    }

    @Test
    public void testRepositoryCheckNoCommand() {

        var repository = Mockito.mock(ReplicaRepository.class);
        var replicaStore = Mockito.mock(ReplicaStore.class);
        var account = new Account();
        when(repository.getState()).thenReturn(ReplicaRepository.State.OPEN);
        when(replicaStore.isOk()).thenReturn(FileStoreState.OK);

        var check = spy(new CheckHealthTask());
        check.setAccount(account);
        check.setReplicaStore(replicaStore);
        check.setRepository(repository);

        check.run();

        verify(replicaStore).isOk();
        verify(check, never()).checkHealthCommand();
    }


    @Test
    public void testSetReadOnlyOnCommandExit_1() {

        var repository = Mockito.mock(ReplicaRepository.class);
        var replicaStore = Mockito.mock(ReplicaStore.class);
        var account = new Account();

        when(repository.getState()).thenReturn(ReplicaRepository.State.OPEN);
        var storeState = ArgumentCaptor.forClass(FaultAction.class);


        var check = new CheckHealthTask();
        check.setAccount(account);
        check.setReplicaStore(replicaStore);
        check.setRepository(repository);
        check.setCommand("/bin/sh -c 'exit 1'");

        check.run();

        verify(repository).fail(storeState.capture(), any());
        assertEquals(FaultAction.READONLY, storeState.getValue());
    }

    @Test
    public void testDontFailOnCommandExit_0() {

        var repository = Mockito.mock(ReplicaRepository.class);
        var replicaStore = Mockito.mock(ReplicaStore.class);
        var account = new Account();

        when(repository.getState()).thenReturn(ReplicaRepository.State.OPEN);

        var check = new CheckHealthTask();
        check.setAccount(account);
        check.setReplicaStore(replicaStore);
        check.setRepository(repository);
        check.setCommand("/bin/sh -c 'exit 0'");

        check.run();

        verify(repository, never()).fail(any(), any());
    }

    @Test
    public void testDisableOnCommandExit_any() {

        var repository = Mockito.mock(ReplicaRepository.class);
        var replicaStore = Mockito.mock(ReplicaStore.class);
        var account = new Account();

        when(repository.getState()).thenReturn(ReplicaRepository.State.OPEN);
        var storeState = ArgumentCaptor.forClass(FaultAction.class);


        var check = new CheckHealthTask();
        check.setAccount(account);
        check.setReplicaStore(replicaStore);
        check.setRepository(repository);
        check.setCommand("/bin/sh -c 'exit 2'");

        check.run();

        verify(repository).fail(storeState.capture(), any());
        assertEquals(FaultAction.DISABLED, storeState.getValue());
    }



    @Test
    public void testSetReadonlyOnCheckRO() {

        var repository = Mockito.mock(ReplicaRepository.class);
        var replicaStore = Mockito.mock(ReplicaStore.class);
        var account = new Account();
        when(repository.getState()).thenReturn(ReplicaRepository.State.OPEN);
        when(replicaStore.isOk()).thenReturn(FileStoreState.READ_ONLY);

        var storeState = ArgumentCaptor.forClass(FaultAction.class);

        var check = spy(new CheckHealthTask());
        check.setAccount(account);
        check.setReplicaStore(replicaStore);
        check.setRepository(repository);

        check.run();

        verify(repository).fail(storeState.capture(), any());
        assertEquals(FaultAction.READONLY, storeState.getValue());
    }

    @Test
    public void testSetDisableOnFailed() {

        var repository = Mockito.mock(ReplicaRepository.class);
        var replicaStore = Mockito.mock(ReplicaStore.class);
        var account = new Account();
        when(repository.getState()).thenReturn(ReplicaRepository.State.OPEN);
        when(replicaStore.isOk()).thenReturn(FileStoreState.FAILED);

        var storeState = ArgumentCaptor.forClass(FaultAction.class);

        var check = spy(new CheckHealthTask());
        check.setAccount(account);
        check.setReplicaStore(replicaStore);
        check.setRepository(repository);

        check.run();

        verify(repository).fail(storeState.capture(), any());
        assertEquals(FaultAction.DISABLED, storeState.getValue());
    }

    @Test
    public void testSetDisableOnTestFailure() {

        var repository = Mockito.mock(ReplicaRepository.class);
        var replicaStore = Mockito.mock(ReplicaStore.class);
        var account = new Account();
        when(repository.getState()).thenReturn(ReplicaRepository.State.OPEN);

        var storeState = ArgumentCaptor.forClass(FaultAction.class);

        var check = spy(new CheckHealthTask());
        check.setAccount(account);
        check.setReplicaStore(replicaStore);
        check.setRepository(repository);
        check.setCommand("/non/existing/command");

        check.run();

        verify(repository).fail(storeState.capture(), any());
        assertEquals(FaultAction.DISABLED, storeState.getValue());
    }

}
