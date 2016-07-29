package dmg.cells.nucleus;

import com.google.common.collect.Collections2;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;

/**
 * The CellPath is an abstraction of the path a CellMessage is
 * assumed to travel. The path consists of a defined sequence of
 * cell hops and a current position. The last hop which might
 * as well be the only one, is called the FinalDestination.
 * At any point a new Cell Hop can be added in two ways :
 * <ul>
 * <li>At the end of the sequence. The added Cell becomes the
 * new FinalDestination.
 * <li>Insert the new Cell behind the current position. The new
 * Hop becomes the next hop.
 * </ul>
 * The string representation of a cell path can have the format:
 * <pre>
 *    path :  &lt;addr1&gt;[:&lt;&lt;addr2&gt;[...]]
 *    addr :  &lt;cellName&gt; | &lt;cellName@domainName&gt; | &lt;cellName@local&gt;
 *  </pre>
 *
 * @author Patrick Fuhrmann
 * @version 0.1, 15 Feb 1998
 */
public final class CellPath implements Cloneable, Serializable
{
    private static final long serialVersionUID = -4922955783102747577L;

    private final List<CellAddressCore> _list;
    private int _position = -1;

    private CellPath(int position, List<CellAddressCore> list)
    {
        checkArgument(position >= -1 && position < list.size());
        _position = position;
        _list = list;
    }

    protected CellPath()
    {
        this(-1, new ArrayList<>());
    }

    public CellPath(String path)
    {
        this(0, streamOfPath(path).collect(toList()));
    }

    public CellPath(CellAddressCore... address)
    {
        this(0, Lists.newArrayList(address));
    }

    public CellPath(CellPath path, CellAddressCore... addresses)
    {
        this(0, Stream.concat(path._list.stream(), Stream.of(addresses)).collect(toList()));
    }

    public CellPath(String cellName, String domainName)
    {
        this(new CellAddressCore(cellName, domainName));
    }

    public synchronized int hops()
    {
        return _list.size();
    }

    public synchronized void add(CellAddressCore core)
    {
        _list.add(core);
        if (_position < 0) {
            _position = 0;
        }
    }

    public synchronized void add(CellPath addr)
    {
        _list.addAll(addr._list);
        if (_position < 0) {
            _position = 0;
        }
    }

    /**
     * Adds a cell path &lt;path&gt; to the end of the current path.
     *
     * @param path The added cell travel path.
     */
    public synchronized void add(String path)
    {
        streamOfPath(path).forEachOrdered(this::add);
    }

    @Override
    public synchronized CellPath clone()
    {
        return new CellPath(_position, new ArrayList<>(_list));
    }

    /**
     * Adds a cell path &lt;path&gt; at the current position.
     */
    public synchronized void insert(CellPath path)
    {
        _list.addAll(_position, path._list);
    }

    /**
     * Increment the current cell position by one.
     *
     * @return true if the path was not at the last position before the call, false otherwise.
     */
    public synchronized boolean next()
    {
        int size = _list.size();
        if (_position < size) {
            _position++;
        }
        return _position < size;
    }

    /**
     * Returns a new path that is the reverse of this path.
     *
     * The new path will have been collapsed such that only cells are
     * addressed. Domain addresses will have been stripped, except if
     * followed by a local (not fully qualified) address.
     */
    public synchronized CellPath revert()
    {
        CellPath path = new CellPath();
        Iterator<CellAddressCore> iterator = Lists.reverse(_list).iterator();
        if (iterator.hasNext()) {
            CellAddressCore address = iterator.next();
            while (iterator.hasNext()) {
                CellAddressCore next = iterator.next();
                if (!address.isDomainAddress() || next.isLocalAddress()) {
                    path.add(address);
                }
                address = next;
            }
            path.add(address);
        }
        return path;
    }

    public synchronized boolean isFinalDestination()
    {
        return _position >= _list.size();
    }

    public synchronized boolean isFirstDestination()
    {
        return _position == 0;
    }

    public synchronized CellAddressCore getCurrent()
    {
        if ((_list.size() == 0) ||
            (_position < 0) ||
            (_position >= _list.size())) {
            return null;
        }
        return _list.get(_position);
    }

    public synchronized CellAddressCore getSourceAddress()
    {
        return _list.get(0);
    }

    public synchronized CellAddressCore getDestinationAddress()
    {
        return _list.get(_list.size() - 1);
    }

    synchronized void replaceCurrent(CellAddressCore core)
    {
        if ((_list.size() == 0) ||
            (_position < 0) ||
            (_position >= _list.size())) {
            return;
        }
        _list.set(_position, core);
    }

    public String getCellName()
    {
        CellAddressCore core = getCurrent();
        return core == null ? null : core.getCellName();
    }

    public String getCellDomainName()
    {
        CellAddressCore core = getCurrent();
        return core == null ? null : core.getCellDomainName();
    }

    public synchronized String toSmallString()
    {
        int size = _list.size();
        if (size == 0) {
            return "[empty]";
        }
        if ((_position >= size) || (_position < 0)) {
            return "[INVALID]";
        }

        CellAddressCore core = _list.get(_position);

        if (size == 1) {
            return "[" + core.toString() + "]";
        }

        if (_position == 0) {
            return "[" + core.toString() + ":...(" + (size - 1) + ")...]";
        }
        if (_position == (size - 1)) {
            return "[...(" + (size - 1) + ")...:" + core.toString() + "]";
        }

        return "[...(" + _position + ")...:" +
               core.toString() +
               "...(" + (size - _position - 1) + ")...]";
    }

    /**
     * Returns the cell path as a colon separated list of addresses. This is the same format
     * accepted by the string constructor of CellPath.
     */
    public String toAddressString()
    {
        return _list.stream().map(CellAddressCore::toString).collect(joining(":"));
    }

    public List<CellAddressCore> getAddresses()
    {
        return Collections.unmodifiableList(_list);
    }

    @Override
    public String toString()
    {
        return toFullString();
    }

    public synchronized String toFullString()
    {
        int size = _list.size();
        if (size == 0) {
            return "[empty]";
        }
        int position = _position;
        if (position > size || position < 0) {
            return "[INVALID]";
        }

        StringBuilder sb = new StringBuilder(size * 16);

        sb.append("[");

        CellAddressCore address;
        int i = 0;
        if (position < size) {
            for (; i < position; i++) {
                address = _list.get(i);
                sb.append(address.getCellName()).append("@").append(address.getCellDomainName());
                sb.append(":");
            }
            sb.append(">");
        }
        address = _list.get(i);
        sb.append(address.getCellName()).append("@").append(address.getCellDomainName());
        for (i++; i < size; i++) {
            sb.append(":");
            address = _list.get(i);
            sb.append(address.getCellName()).append("@").append(address.getCellDomainName());
        }
        sb.append("]");
        return sb.toString();
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj == this) {
            return true;
        }

        if (!(obj instanceof CellPath)) {
            return false;
        }

        List<CellAddressCore> other;
        synchronized (obj) {
            other = new ArrayList<>(((CellPath) obj)._list);
        }

        synchronized (this) {
            return _list.equals(other);
        }
    }

    @Override
    public synchronized int hashCode()
    {
        /* Beware that equals only takes the list of addresses into account.
         */
        return _list.hashCode();
    }

    private static Stream<CellAddressCore> streamOfPath(String path)
    {
        checkArgument(!path.isEmpty());
        return Stream.of(path.split(":")).map(CellAddressCore::new);
    }

    public boolean contains(CellAddressCore address)
    {
        return _list.contains(address);
    }
}
