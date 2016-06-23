#set( $symbol_pound = '#' )
#set( $symbol_dollar = '$' )
#set( $symbol_escape = '\' )
package ${package};

import java.util.Map;
import java.util.UUID;

import org.dcache.pool.nearline.spi.FlushRequest;
import org.dcache.pool.nearline.spi.NearlineStorage;
import org.dcache.pool.nearline.spi.RemoveRequest;
import org.dcache.pool.nearline.spi.StageRequest;

public class PluginNearlineStorage implements NearlineStorage
{
    protected final String type;
    protected final String name;

    public PluginNearlineStorage(String type, String name)
    {
        this.type = type;
        this.name = name;
    }

    /**
     * Flush all files in {@code requests} to nearline storage.
     *
     * @param requests
     */
    @Override
    public void flush(Iterable<FlushRequest> requests)
    {
        throw new UnsupportedOperationException("Not implemented");
    }

    /**
     * Stage all files in {@code requests} from nearline storage.
     *
     * @param requests
     */
    @Override
    public void stage(Iterable<StageRequest> requests)
    {
        throw new UnsupportedOperationException("Not implemented");
    }

    /**
     * Delete all files in {@code requests} from nearline storage.
     *
     * @param requests
     */
    @Override
    public void remove(Iterable<RemoveRequest> requests)
    {
        throw new UnsupportedOperationException("Not implemented");
    }

    /**
     * Cancel any flush, stage or remove request with the given id.
     * <p>
     * The failed method of any cancelled request should be called with a
     * CancellationException. If the request completes before it can be
     * cancelled, then the cancellation should be ignored and the completed
     * or failed method should be called as appropriate.
     * <p>
     * A call to cancel must be non-blocking.
     *
     * @param uuid id of the request to cancel
     */
    @Override
    public void cancel(UUID uuid)
    {

    }

    /**
     * Applies a new configuration.
     *
     * @param properties
     * @throws IllegalArgumentException if the configuration is invalid
     */
    @Override
    public void configure(Map<String, String> properties) throws IllegalArgumentException
    {

    }

    /**
     * Cancels all requests and initiates a shutdown of the nearline storage
     * interface.
     * <p>
     * This method does not wait for actively executing requests to
     * terminate.
     */
    @Override
    public void shutdown()
    {

    }
}
