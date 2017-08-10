package diskCacheV111.vehicles;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.*;

/**
 * Created by alenaschemmert on 03.08.17.
 */

public class JobInfoTests {

    private JobInfo jobInfo;
    private OldJobInfo oldJobInfo;
    private String ref;

    @Before
    public void setUp() {
        jobInfo = new JobInfo(System.currentTimeMillis(), System.currentTimeMillis() + 1000, "working", 1, "waldo", 2407);
        oldJobInfo = new OldJobInfo(System.currentTimeMillis(), System.currentTimeMillis() + 1000, "working", 1, "waldo", 2407);
        ref = jobInfo.toString();
    }

    @Test
    public void toStringShouldBeThreadSafe() {
        Callable<String> task = jobInfo::toString;
        ExecutorService exec = Executors.newFixedThreadPool(8);
        List<Future<String>> results = new ArrayList<>();

        for (int i = 0; i < 8; i++) {
            results.add(exec.submit(task));
        }
        exec.shutdown();

        for (Future<String> result : results) {
            try {
                Assert.assertEquals(ref, result.get());
            } catch (InterruptedException | ExecutionException e) {
                Assert.assertFalse(true);
            }
        }
    }

    @Test
    public void OldAndNewShouldBeEqual() {
        Assert.assertEquals(jobInfo.toString(), oldJobInfo.toString());
    }

    private static class OldJobInfo extends JobInfo {
        private static final SimpleDateFormat __format = new SimpleDateFormat("MM/dd-HH:mm:ss");

        OldJobInfo(long submitTime, long startTime, String status, int id, String clientName, long clientId) {
            super(submitTime, startTime, status, id, clientName, clientId);
        }

        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append(getJobId()).append(';');
            sb.append(getClientName()).append(':').append(getClientId());
            synchronized (__format) {
                sb.append(';').append(__format.format(new Date(getStartTime()))).
                        append(';').append(__format.format(new Date(getSubmitTime()))).
                        append(';').append(getStatus()).append(';') ;
            }
            return sb.toString();
        }
    }
}
