package org.dcache.srm.scheduler;

import com.google.common.collect.Lists;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.List;

import org.dcache.srm.request.BringOnlineFileRequest;
import org.dcache.srm.request.BringOnlineRequest;
import org.dcache.srm.request.CopyFileRequest;
import org.dcache.srm.request.CopyRequest;
import org.dcache.srm.request.GetFileRequest;
import org.dcache.srm.request.GetRequest;
import org.dcache.srm.request.Job;
import org.dcache.srm.request.LsFileRequest;
import org.dcache.srm.request.LsRequest;
import org.dcache.srm.request.PutFileRequest;
import org.dcache.srm.request.PutRequest;
import org.dcache.srm.request.ReserveSpaceRequest;

import static org.dcache.srm.scheduler.State.ASYNCWAIT;
import static org.dcache.srm.scheduler.State.PENDING;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertThat;
import static org.mockito.BDDMockito.*;

@RunWith(PowerMockRunner.class)
@PrepareForTest({Scheduler.class, CopyRequest.class, CopyFileRequest.class,
    GetRequest.class, GetFileRequest.class, PutRequest.class, PutFileRequest.class,
    BringOnlineRequest.class, BringOnlineFileRequest.class,
    LsRequest.class, LsFileRequest.class, ReserveSpaceRequest.class})
public class SchedulerContainerTests
{
    SchedulerContainer container;
    Scheduler getScheduler;
    Scheduler lsScheduler;
    Scheduler putScheduler;
    Scheduler bringOnlineScheduler;
    Scheduler reserveSpaceScheduler;
    Scheduler genericScheduler;

    private Scheduler mockScheduler(Class<? extends Job> type, String id)
    {
        Scheduler scheduler = PowerMockito.mock(Scheduler.class);
        given(scheduler.getType()).willReturn(type);
        given(scheduler.getId()).willReturn(id);
        return scheduler;
    }

    private <T extends Job> T mockJob(Class<T> type)
    {
        return mockJob(type, PENDING, null);
    }

    private <T extends Job> T mockJob(Class<T> type, State state, String schedulerId)
    {
        T job = PowerMockito.mock(type);
        given(job.getSchedulerType()).willCallRealMethod();
        given(job.getState()).willReturn(state);
        given(job.getSchedulerId()).willReturn(schedulerId);

        return job;
    }

    @Before
    public void setup()
    {
        getScheduler = mockScheduler(GetFileRequest.class, "get_localhost");
        lsScheduler = mockScheduler(LsFileRequest.class, "ls_localhost");
        putScheduler = mockScheduler(PutFileRequest.class, "put_localhost");
        bringOnlineScheduler = mockScheduler(BringOnlineFileRequest.class, "bring_online_localhost");
        reserveSpaceScheduler = mockScheduler(ReserveSpaceRequest.class, "reserve_space_localhost");
        genericScheduler = mockScheduler(Job.class, "copy_localhost");

        container = new SchedulerContainer();
        List<Scheduler<?>> schedulers = Lists.<Scheduler<?>>newArrayList(getScheduler,
                lsScheduler, putScheduler, bringOnlineScheduler,
                reserveSpaceScheduler, genericScheduler);
        container.setSchedulers(schedulers);
    }

    /* Check correct scheduler is selected when new Job is created */

    @Test
    public void shouldScheduleCopyRequest() throws Exception
    {
        CopyRequest job = mockJob(CopyRequest.class);
        ArgumentCaptor<Scheduler> schedCapture = ArgumentCaptor.forClass(Scheduler.class);

        container.schedule(job);

        verify(job, times(1)).scheduleWith(schedCapture.capture());
        assertThat(schedCapture.getValue(), is(genericScheduler));
    }

    @Test
    public void shouldScheduleCopyFileRequest() throws Exception
    {
        CopyFileRequest job = mockJob(CopyFileRequest.class);
        ArgumentCaptor<Scheduler> schedCapture = ArgumentCaptor.forClass(Scheduler.class);

        container.schedule(job);

        verify(job, times(1)).scheduleWith(schedCapture.capture());
        assertThat(schedCapture.getValue(), is(genericScheduler));
    }

    @Test
    public void shouldScheduleGetRequest() throws Exception
    {
        GetRequest job = mockJob(GetRequest.class);
        ArgumentCaptor<Scheduler> schedCapture = ArgumentCaptor.forClass(Scheduler.class);

        container.schedule(job);

        verify(job, times(1)).scheduleWith(schedCapture.capture());
        assertThat(schedCapture.getValue(), is(getScheduler));
    }

    @Test
    public void shouldScheduleGetFileRequest() throws Exception
    {
        GetFileRequest job = mockJob(GetFileRequest.class);
        ArgumentCaptor<Scheduler> schedCapture = ArgumentCaptor.forClass(Scheduler.class);

        container.schedule(job);

        verify(job, times(1)).scheduleWith(schedCapture.capture());
        assertThat(schedCapture.getValue(), is(getScheduler));
    }

    @Test
    public void shouldScheduleBringOnlineRequest() throws Exception
    {
        BringOnlineRequest job = mockJob(BringOnlineRequest.class);
        ArgumentCaptor<Scheduler> schedCapture = ArgumentCaptor.forClass(Scheduler.class);

        container.schedule(job);

        verify(job, times(1)).scheduleWith(schedCapture.capture());
        assertThat(schedCapture.getValue(), is(bringOnlineScheduler));
    }

    @Test
    public void shouldScheduleBringOnlineFileRequest() throws Exception
    {
        BringOnlineFileRequest job = mockJob(BringOnlineFileRequest.class);
        ArgumentCaptor<Scheduler> schedCapture = ArgumentCaptor.forClass(Scheduler.class);

        container.schedule(job);

        verify(job, times(1)).scheduleWith(schedCapture.capture());
        assertThat(schedCapture.getValue(), is(bringOnlineScheduler));
    }

    @Test
    public void shouldScheduleLsFileRequest() throws Exception
    {
        LsFileRequest job = mockJob(LsFileRequest.class);
        ArgumentCaptor<Scheduler> schedCapture = ArgumentCaptor.forClass(Scheduler.class);

        container.schedule(job);

        verify(job, times(1)).scheduleWith(schedCapture.capture());
        assertThat(schedCapture.getValue(), is(lsScheduler));
    }

    @Test
    public void shouldScheduleLsRequest() throws Exception
    {
        LsRequest job = mockJob(LsRequest.class);
        ArgumentCaptor<Scheduler> schedCapture = ArgumentCaptor.forClass(Scheduler.class);

        container.schedule(job);

        verify(job, times(1)).scheduleWith(schedCapture.capture());
        assertThat(schedCapture.getValue(), is(lsScheduler));
    }


    @Test
    public void shouldSchedulePutFileRequest() throws Exception
    {
        PutFileRequest job = mockJob(PutFileRequest.class);
        ArgumentCaptor<Scheduler> schedCapture = ArgumentCaptor.forClass(Scheduler.class);

        container.schedule(job);

        verify(job, times(1)).scheduleWith(schedCapture.capture());
        assertThat(schedCapture.getValue(), is(putScheduler));
    }

    @Test
    public void shouldSchedulePutRequest() throws Exception
    {
        PutRequest job = mockJob(PutRequest.class);
        ArgumentCaptor<Scheduler> schedCapture = ArgumentCaptor.forClass(Scheduler.class);

        container.schedule(job);

        verify(job, times(1)).scheduleWith(schedCapture.capture());
        assertThat(schedCapture.getValue(), is(putScheduler));
    }

    @Test
    public void shouldScheduleReserveSpaceRequest() throws Exception
    {
        ReserveSpaceRequest job = mockJob(ReserveSpaceRequest.class);
        ArgumentCaptor<Scheduler> schedCapture = ArgumentCaptor.forClass(Scheduler.class);

        container.schedule(job);

        verify(job, times(1)).scheduleWith(schedCapture.capture());
        assertThat(schedCapture.getValue(), is(reserveSpaceScheduler));
    }


    /* Check correct scheduler is selected when restoring a Job after SRM restart */

    /* We don't test most of the container requests as they don't do any
     * scheduling on SRM restart */

    @Test
    public void shouldScheduleRestoredLsFileRequests() throws Exception
    {
        LsFileRequest job = mockJob(LsFileRequest.class, ASYNCWAIT, "ls_localhost");

        container.restoreJobsOnSrmStart(Lists.newArrayList(job));

        ArgumentCaptor<Scheduler> schedCapture = ArgumentCaptor.forClass(Scheduler.class);
        verify(job, times(1)).onSrmRestart(schedCapture.capture());
        assertThat(schedCapture.getValue(), is(lsScheduler));
    }

    @Test
    public void shouldScheduleRestoredBringOnlineFileRequests() throws Exception
    {
        BringOnlineFileRequest job = mockJob(BringOnlineFileRequest.class,
                ASYNCWAIT, "bring_online_localhost");

        container.restoreJobsOnSrmStart(Lists.newArrayList(job));

        ArgumentCaptor<Scheduler> schedCapture = ArgumentCaptor.forClass(Scheduler.class);
        verify(job, times(1)).onSrmRestart(schedCapture.capture());
        assertThat(schedCapture.getValue(), is(bringOnlineScheduler));
    }

    @Test
    public void shouldScheduleRestoredGetFileRequests() throws Exception
    {
        GetFileRequest job = mockJob(GetFileRequest.class, ASYNCWAIT,
                "get_localhost");

        container.restoreJobsOnSrmStart(Lists.newArrayList(job));

        ArgumentCaptor<Scheduler> schedCapture = ArgumentCaptor.forClass(Scheduler.class);
        verify(job, times(1)).onSrmRestart(schedCapture.capture());
        assertThat(schedCapture.getValue(), is(getScheduler));
    }

    @Test
    public void shouldScheduleRestoredCopyRequests() throws Exception
    {
        CopyRequest job = mockJob(CopyRequest.class, ASYNCWAIT, "copy_localhost");

        container.restoreJobsOnSrmStart(Lists.newArrayList(job));

        ArgumentCaptor<Scheduler> schedCapture = ArgumentCaptor.forClass(Scheduler.class);
        verify(job, times(1)).onSrmRestart(schedCapture.capture());
        assertThat(schedCapture.getValue(), is(genericScheduler));
    }

    @Test
    public void shouldScheduleRestoredCopyFileRequests() throws Exception
    {
        CopyFileRequest job = mockJob(CopyFileRequest.class, ASYNCWAIT,
                "copy_localhost");

        container.restoreJobsOnSrmStart(Lists.newArrayList(job));

        ArgumentCaptor<Scheduler> schedCapture = ArgumentCaptor.forClass(Scheduler.class);
        verify(job, times(1)).onSrmRestart(schedCapture.capture());
        assertThat(schedCapture.getValue(), is(genericScheduler));
    }

    @Test
    public void shouldScheduleRestoredPutFileRequests() throws Exception
    {
        PutFileRequest job = mockJob(PutFileRequest.class, ASYNCWAIT,
                "put_localhost");

        container.restoreJobsOnSrmStart(Lists.newArrayList(job));

        ArgumentCaptor<Scheduler> schedCapture = ArgumentCaptor.forClass(Scheduler.class);
        verify(job, times(1)).onSrmRestart(schedCapture.capture());
        assertThat(schedCapture.getValue(), is(putScheduler));
    }

    @Test
    public void shouldScheduleRestoredReserveSpaceRequests() throws Exception
    {
        ReserveSpaceRequest job = mockJob(ReserveSpaceRequest.class, ASYNCWAIT,
                "reserve_space_localhost");

        container.restoreJobsOnSrmStart(Lists.newArrayList(job));

        ArgumentCaptor<Scheduler> schedCapture = ArgumentCaptor.forClass(Scheduler.class);
        verify(job, times(1)).onSrmRestart(schedCapture.capture());
        assertThat(schedCapture.getValue(), is(reserveSpaceScheduler));
    }
}
