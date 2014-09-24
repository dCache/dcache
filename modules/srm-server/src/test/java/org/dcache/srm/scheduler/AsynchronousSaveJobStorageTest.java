package org.dcache.srm.scheduler;

import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;

import org.dcache.srm.request.Job;

import static org.mockito.Mockito.*;

public class AsynchronousSaveJobStorageTest
{
    private JobStorage<Job> storage;
    private List<Runnable> tasks;
    private AsynchronousSaveJobStorage<Job> asyncStorage;
    private Job job;

    @Before
    public void setUp() throws Exception
    {
        storage = mock(JobStorage.class);
        job = mock(Job.class);
        tasks = new ArrayList<>();
        asyncStorage = new AsynchronousSaveJobStorage<>(storage, new ListExecutor(tasks));
    }

    @Test
    public void whenRequestHistoryLoggingIsDisabledAndSavingWithForceThenActualSaveIsWithForce() throws Exception
    {
        when(storage.isJdbcLogRequestHistoryInDBEnabled()).thenReturn(false);
        asyncStorage.saveJob(job, true);
        runTasks();
        verify(storage).saveJob(job, true);
    }

    @Test
    public void whenRequestHistoryLoggingIsEnabledAndSavingWithForceThenActualSaveIsWithForce() throws Exception
    {
        when(storage.isJdbcLogRequestHistoryInDBEnabled()).thenReturn(true);
        asyncStorage.saveJob(job, true);
        runTasks();
        verify(storage).saveJob(job, true);
    }

    @Test
    public void whenRequestHistoryLoggingIsEnabledAndSavingWithoutForceThenActualSaveIsWithoutForce() throws Exception
    {
        when(storage.isJdbcLogRequestHistoryInDBEnabled()).thenReturn(true);
        asyncStorage.saveJob(job, false);
        runTasks();
        verify(storage).saveJob(job, false);
    }

    @Test
    public void whenSavingTwiceWithoutForceThenActualSaveIsOnceWithoutForce() throws Exception
    {
        when(storage.isJdbcLogRequestHistoryInDBEnabled()).thenReturn(true);
        asyncStorage.saveJob(job, false);
        asyncStorage.saveJob(job, false);
        runTasks();
        verify(storage).saveJob(job, false);
    }

    @Test
    public void whenSavingTwiceWithForceThenActualSaveIsOnceWithForce() throws Exception
    {
        when(storage.isJdbcLogRequestHistoryInDBEnabled()).thenReturn(true);
        asyncStorage.saveJob(job, true);
        asyncStorage.saveJob(job, true);
        runTasks();
        verify(storage).saveJob(job, true);
    }

    @Test
    public void whenSavingTwiceWithAndWithoutForceThenActualSaveIsOnceWithForce() throws Exception
    {
        when(storage.isJdbcLogRequestHistoryInDBEnabled()).thenReturn(true);
        asyncStorage.saveJob(job, true);
        asyncStorage.saveJob(job, false);
        runTasks();
        verify(storage).saveJob(job, true);
    }

    @Test
    public void whenSavingTwiceWithoutAndWithForceThenActualSaveIsOnceWithForce() throws Exception
    {
        when(storage.isJdbcLogRequestHistoryInDBEnabled()).thenReturn(true);
        asyncStorage.saveJob(job, false);
        asyncStorage.saveJob(job, true);
        runTasks();
        verify(storage).saveJob(job, true);
    }

    @Test
    public void whenExecutionQueueIsFullForcedSaveIsStillExecuted() throws Exception
    {
        Executor executor = mock(Executor.class);
        asyncStorage = new AsynchronousSaveJobStorage<>(storage, executor);
        when(storage.isJdbcLogRequestHistoryInDBEnabled()).thenReturn(true);
        doThrow(RejectedExecutionException.class).when(executor).execute(any(Runnable.class));
        asyncStorage.saveJob(job, true);
        verify(storage).saveJob(job, true);
    }

    private void runTasks()
    {
        for (Runnable task : tasks) {
            task.run();
        }
    }

    private static class ListExecutor implements Executor
    {
        private final List<Runnable> tasks;

        private ListExecutor(List<Runnable> tasks)
        {
            this.tasks = tasks;
        }

        @Override
        public void execute(
                Runnable command)
        {
            tasks.add(command);
        }
    }
}