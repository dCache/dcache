package diskCacheV111.vehicles;

import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;

public class JobInfoTests {

    private JobInfo jobInfo;

    @Before
    public void setUp() {
        long time = 1502366668987L;
        jobInfo = new JobInfo(time, time + 1000, "RUNNING", 1, "waldo", 2407);
    }

    @Test
    public void newToStringShouldBeThreadSafe() {
        ExecutorService exec = Executors.newFixedThreadPool(8);
        List<Future<String>> results = new ArrayList<>();

        for (int i = 0; i < 8; i++) {
            results.add(exec.submit(jobInfo::toString));
        }
        exec.shutdown();
        for (Future<String> result : results) {
            try {
                assertEquals("1;waldo:2407;08/10-14:04:28;08/10-14:04:29;RUNNING;", result.get());
            } catch (InterruptedException | ExecutionException e) {
                fail();
            }
        }
    }

    @Test
    public void oldAndNewToStringShouldBeEqual() {
        assertThat(jobInfo.toString(), is("1;waldo:2407;08/10-14:04:28;08/10-14:04:29;RUNNING;"));
    }


}
