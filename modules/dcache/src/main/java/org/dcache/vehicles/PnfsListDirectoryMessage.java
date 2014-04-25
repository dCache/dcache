package org.dcache.vehicles;

import com.google.common.collect.BoundType;
import com.google.common.collect.Range;

import java.util.Collection;
import java.util.Set;
import java.util.UUID;

import diskCacheV111.namespace.NameSpaceProvider;
import diskCacheV111.vehicles.Message;
import diskCacheV111.vehicles.PnfsMessage;

import dmg.util.CollectionFactory;

import org.dcache.namespace.FileAttribute;
import org.dcache.util.Glob;
import org.dcache.util.list.DirectoryEntry;

/**
 * Requests a directory listing. Since the result set may be very
 * large, a single request may result in multiple replies. The request
 * is identified by a UUID and the replies will contain the same
 * UUID. The last reply is flagged as final. It is assumed that
 * point-to-point message ordering is guaranteed.
 */
public class PnfsListDirectoryMessage extends PnfsMessage
{
    private static final long serialVersionUID = -5774904472984157638L;

    private final Glob _pattern;
    private final Integer _lower;
    private final Integer _upper;
    private final BoundType _lowerBoundType;
    private final BoundType _upperBoundType;
    private final UUID _uuid = UUID.randomUUID();
    private final Set<FileAttribute> _requestedAttributes;
    private Collection<DirectoryEntry> _entries =
        CollectionFactory.newArrayList();

    /**
     * The last message has the following field set to true and a non-zero
     * message count;
     */
    private boolean _isFinal;
    private int _messageCount;

    /**
     * Constructs a new message.
     *
     * @param path The full PNFS path of the directory to list
     * @param pattern Optional glob pattern for filtering the result
     * @param range Range for bracketing the result
     * @param attr The file attributes to include for each entry
     * @see NameSpaceProvider#list
     */
    public PnfsListDirectoryMessage(String path, Glob pattern,
                                    Range<Integer> range,
                                    Set<FileAttribute> attr)
    {
        setReplyRequired(true);
        setPnfsPath(path);
        _pattern = pattern;
        _lower = range.hasLowerBound() ? range.lowerEndpoint() : null;
        _upper = range.hasUpperBound() ? range.upperEndpoint() : null;
        _lowerBoundType = range.hasLowerBound() ? range.lowerBoundType() : null;
        _upperBoundType = range.hasUpperBound() ? range.upperBoundType() : null;
        _requestedAttributes = attr;
    }

    /** Returns the UUID identifying this request. */
    public UUID getUUID()
    {
        return _uuid;
    }

    /** Returns the pattern used to filter the result. */
    public Glob getPattern()
    {
        return _pattern;
    }

    /** Returns the optional range bracketing the result. */
    public Range<Integer> getRange()
    {
        if (_lower == null && _upper == null) {
            return Range.all();
        } else if (_lower == null) {
            return Range.upTo(_upper, _upperBoundType);
        } else if (_upper == null) {
            return Range.downTo(_lower, _lowerBoundType);
        } else {
            return Range.range(_lower, _lowerBoundType,
                               _upper, _upperBoundType);
        }
    }

    /** True if and only if the reply should include file meta data. */
    public Set<FileAttribute> getRequestedAttributes()
    {
        return _requestedAttributes;
    }

    /** Adds an entry to the entry list. */
    public void addEntry(String name, FileAttributes attr)
    {
        _entries.add(new DirectoryEntry(name, attr));
    }

    /** Sets a new entry list. */
    public void setEntries(Collection<DirectoryEntry> entries)
    {
        _entries = entries;
    }

    /** Returns the entry list. */
    public Collection<DirectoryEntry> getEntries()
    {
        return _entries;
    }

    /** Clears the entry list. */
    public void clear()
    {
        _entries.clear();
    }

    @Override
    public void setSucceeded()
    {
        super.setSucceeded();
        _isFinal = true;
    }

    public void setSucceeded(int messageCount)
    {
        setSucceeded();
        _messageCount = messageCount;
    }

    public boolean isFinal()
    {
        return _isFinal;
    }

    public int getMessageCount()
    {
        return _messageCount;
    }

    @Override
    public boolean invalidates(Message message)
    {
        return false;
    }
}
