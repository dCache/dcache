package diskCacheV111.vehicles;

import java.io.Serializable;
import java.net.InetSocketAddress;

public class PoolPassiveIoFileMessage<T extends Serializable> extends PoolMessage {

    private static final long serialVersionUID = -8019787998659861618L;
    private final InetSocketAddress _socketAddress;
    private final InetSocketAddress[] _socketAddresses;
    private final T _challange;

    /**
     * Pool's restart verifier.
     */
    private final long _verifier;

    public PoolPassiveIoFileMessage(String pool, InetSocketAddress socketAddress, T challenge) {
        this(pool, new InetSocketAddress[] { socketAddress }, challenge, 0);
    }

    /**
     * Create message for given {@code pool}, {@code socketAddresses}, {@code challenge} and
     * {@code boot verifier}.
     *
     * @param pool
     * @param socketAddresses
     * @param challenge
     * @param verifier
     */
    public PoolPassiveIoFileMessage(String pool, InetSocketAddress[] socketAddresses, T challenge,  long verifier) {
        super(pool);
        _socketAddresses = socketAddresses;
        _socketAddress = _socketAddresses[0];
        _challange = challenge;
        _verifier = verifier;
    }

    public InetSocketAddress socketAddress() {
        return _socketAddress;
    }

    public InetSocketAddress[] socketAddresses() {
        /*
         * A bit of crappy code to handle old/new version of the class.
         */
        return _socketAddresses == null?
                new InetSocketAddress[] { _socketAddress } : _socketAddresses;
    }

    public long getVerifier() {
        return _verifier;
    }

    public T challange() {
        return _challange;
    }
}
