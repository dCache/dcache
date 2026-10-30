package diskCacheV111.vehicles;

import static org.junit.Assert.assertEquals;

import java.net.InetSocketAddress;
import org.junit.Test;

public class PoolPassiveIoFileMessageTest {

    @Test
    public void shouldDefaultMoverIdToZero() {
        PoolPassiveIoFileMessage<String> msg =
              new PoolPassiveIoFileMessage<>("pool",
                    new InetSocketAddress[]{new InetSocketAddress("127.0.0.1", 2811)},
                    "challenge", 42L);
        assertEquals(0, msg.getMoverId());
    }

    @Test
    public void shouldSetAndGetMoverId() {
        PoolPassiveIoFileMessage<String> msg =
              new PoolPassiveIoFileMessage<>("pool",
                    new InetSocketAddress[]{new InetSocketAddress("127.0.0.1", 2811)},
                    "challenge", 42L);
        msg.setMoverId(12345);
        assertEquals(12345, msg.getMoverId());
    }
}
