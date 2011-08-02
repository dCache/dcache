package dmg.util;

import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author jans
 */
public class GateTest {

    private boolean waitedForOpen = false;
    private boolean enteredAndSlept = false;
    private boolean leftGate = false;
    private boolean openedGate = false;
    private boolean closedGate = false;

    @Before
    public void setUp() {
        waitedForOpen = false;
        enteredAndSlept = false;
        leftGate = false;
        openedGate = false;
        closedGate = false;
    }

    @Test
    public void testGatePassing() {
        int timesWaited = 0;
        GateTestHelper gateTester = new GateTestHelper();
        gateTester.start();
        while (!allStatesReached()) {
            try {
                timesWaited++;
                Thread.sleep(100);
            } catch (InterruptedException ex) {
            }
            if (timesWaited >= 100) {
                gateTester.stopThreads();
                fail("timed out");
            }
        }
    }

    private boolean allStatesReached() {
        return waitedForOpen && enteredAndSlept && leftGate && openedGate && closedGate;
    }

    private class GateTestHelper implements Runnable {

        Thread _ticker1, _ticker2;
        Gate _gate = new Gate(false);

        public GateTestHelper() {
            _ticker1 = new Thread(this);
            _ticker2 = new Thread(this);

        }

        public void start() {
            _ticker1.start();
            _ticker2.start();
        }

        public void stopThreads() {
            _ticker1.interrupt();
            _ticker2.interrupt();
        }

        public void run() {
            try {
                if (Thread.currentThread() == _ticker1) {
                    while (true) {
                        Thread.sleep(250);
                        waitedForOpen = true;
                        synchronized (_gate.check()) {
                            enteredAndSlept = true;
                            Thread.sleep(250);
                            leftGate = true;
                        }
                    }
                } else if (Thread.currentThread() == _ticker2) {
                    while (true) {
                        Thread.sleep(500);
                        _gate.open();
                        openedGate = true;
                        Thread.sleep(500);
                        _gate.close();
                        closedGate = true;
                    }
                }
            } catch (InterruptedException e) {
            }

        }
    }
}
