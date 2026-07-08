package org.dcache.pool.repository;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.emptyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import diskCacheV111.util.PnfsId;
import java.util.Collections;
import java.util.List;
import org.dcache.namespace.FileAttribute;
import org.dcache.vehicles.FileAttributes;
import org.junit.Before;
import org.junit.Test;

/**
 * Regression test for https://github.com/dCache/dcache/issues/8134 : 'rep ls' used to crash with
 * an IllegalStateException when a replica's FileAttributes had no ACCESS_LATENCY defined.
 */
public class RepositoryInterpreterTest {

    private static final PnfsId PNFSID =
          new PnfsId("000000000000000000000000000000000001");

    private RepositoryInterpreter _interpreter;
    private Repository _repository;

    @Before
    public void setUp() {
        _repository = mock(Repository.class);
        _interpreter = new RepositoryInterpreter();
        _interpreter.setRepository(_repository);
    }

    @Test
    public void repLsDoesNotCrashOnUndefinedAccessLatency() throws Exception {
        FileAttributes attributes = new FileAttributes();
        assertThat(attributes.isDefined(FileAttribute.ACCESS_LATENCY), org.hamcrest.Matchers.is(false));

        CacheEntry entry = mock(CacheEntry.class);
        when(entry.getFileAttributes()).thenReturn(attributes);
        when(entry.getState()).thenReturn(ReplicaState.CACHED);
        when(entry.isSticky()).thenReturn(false);
        when(entry.getStickyRecords()).thenReturn(Collections.emptyList());
        when(entry.toString()).thenReturn(PNFSID + " <C------------> 0 si={<unknown>}");

        when(_repository.iterator()).thenReturn(List.of(PNFSID).iterator());
        when(_repository.getEntry(PNFSID)).thenReturn(entry);

        RepositoryInterpreter.ListCommand cmd = _interpreter.new ListCommand();
        cmd.format = "unmanaged";

        // Must not throw IllegalStateException: Attribute is not defined: ACCESS_LATENCY
        String result = (String) cmd.execute();

        verify(_repository).getEntry(PNFSID);
        // Undefined access latency means the replica can't be classified as unmanaged,
        // so it is excluded from the report rather than crashing the command.
        assertThat(result, emptyString());
    }

    @Test
    public void repLsDoesNotCrashOnUndefinedRetentionPolicy() throws Exception {
        FileAttributes attributes = new FileAttributes();
        assertThat(attributes.isDefined(FileAttribute.RETENTION_POLICY), org.hamcrest.Matchers.is(false));

        CacheEntry entry = mock(CacheEntry.class);
        when(entry.getFileAttributes()).thenReturn(attributes);
        when(entry.getState()).thenReturn(ReplicaState.CACHED);
        when(entry.isSticky()).thenReturn(false);
        when(entry.getStickyRecords()).thenReturn(Collections.emptyList());
        when(entry.toString()).thenReturn(PNFSID + " <C------------> 0 si={<unknown>}");

        when(_repository.iterator()).thenReturn(List.of(PNFSID).iterator());
        when(_repository.getEntry(PNFSID)).thenReturn(entry);

        RepositoryInterpreter.ListCommand cmd = _interpreter.new ListCommand();
        cmd.format = "unmanaged";

        // Must not throw IllegalStateException: Attribute is not defined: RETENTION_POLICY
        String result = (String) cmd.execute();

        verify(_repository).getEntry(PNFSID);
        // Undefined retention policy means the replica can't be classified as unmanaged,
        // so it is excluded from the report rather than crashing the command.
        assertThat(result, emptyString());
    }
}
