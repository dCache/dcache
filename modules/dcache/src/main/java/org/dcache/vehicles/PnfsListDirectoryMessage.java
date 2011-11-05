package org.dcache.vehicles;

import java.util.Collection;
import java.util.ArrayList;
import java.util.UUID;
import java.util.Set;
import java.io.Serializable;

import dmg.util.CollectionFactory;

import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.PnfsMessage;
import diskCacheV111.vehicles.Message;
import org.dcache.namespace.FileAttribute;
import org.dcache.util.list.DirectoryEntry;
import org.dcache.util.Glob;
import org.dcache.util.Interval;

/**
 * Requests a directory listing. Since the result set may be very
 * large, a single request may result in multiple replies. The request
 * is identified by a UUID and the replies will contain the same
 * UUID. The last reply is flagged as final. It is assumed that
 * point-to-point message ordering is guaranteed.
 */
public class PnfsListDirectoryMessage extends PnfsMessage
{
    static final long serialVersionUID = -5774904472984157638L;

    public final Glob _pattern;
    public final Interval _range;
    public final UUID _uuid = UUID.randomUUID();
    public final Set<FileAttribute> _requestedAttributes;
    public Collection<DirectoryEntry> _entries =
        CollectionFactory.newArrayList();

    /**
     * The last message has the following field set to true.
     */
    public boolean _isFinal = true;

    /**
     * Constructs a new message.
     *
     * @param path The full PNFS path of the directory to list
     * @param pattern Optional glob pattern for filtering the result
     * @param range Optional range for bracketing the result
     * @param attr The file attributes to include for each entry
     * @see diskCacheV111.namespace.NameSpaceProvider#list
     */
    public PnfsListDirectoryMessage(String path, Glob pattern, Interval range,
                                    Set<FileAttribute> attr)
    {
        setReplyRequired(true);
        setPnfsPath(path);
        _pattern = pattern;
        _range = range;
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

    /** Returns the pattern used to filter the result. */
    public Interval getRange()
    {
        return _range;
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

    public void setFinal(boolean isFinal)
    {
        _isFinal = isFinal;
    }

    public boolean isFinal()
    {
        return _isFinal;
    }

    @Override
    public boolean invalidates(Message message)
    {
        return false;
    }
}
