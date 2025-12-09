/*
 * dCache - http://www.dcache.org/
 *
 * Copyright (C) 2025 Deutsches Elektronen-Synchrotron
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.dcache.vehicles;

import static java.util.Objects.requireNonNull;


import com.google.common.collect.BoundType;
import com.google.common.collect.Range;
import diskCacheV111.vehicles.Message;
import diskCacheV111.vehicles.PnfsMessage;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Set;
import java.util.UUID;

import org.dcache.namespace.FileAttribute;
import org.dcache.util.Glob;
import org.dcache.util.list.DirectoryEntry;


public class PnfsListLabelsMessage extends PnfsMessage {

    private static final long serialVersionUID = -8933898248824453006L;


    private final Integer _lower;
    private final Integer _upper;
    private final BoundType _lowerBoundType;
    private final BoundType _upperBoundType;
    private final UUID _uuid = UUID.randomUUID();
    private final Set<FileAttribute> _requestedAttributes;

    private Collection<DirectoryEntry> _entries = new ArrayList<>();


    /**
     * The last message has the following field set to true and a non-zero message count;
     */
    private boolean _isFinal;
    private int _messageCount = 0;


    /**
     * Constructs a new message.
     *
     * @param range   Range for bracketing the result
     * @param attr    The file attributes to include for each entry
     */
    public PnfsListLabelsMessage(Range<Integer> range,
                                 Set<FileAttribute> attr) {
        setReplyRequired(true);
        _lower = range.hasLowerBound() ? range.lowerEndpoint() : null;
        _upper = range.hasUpperBound() ? range.upperEndpoint() : null;
        _lowerBoundType = range.hasLowerBound() ? range.lowerBoundType() : null;
        _upperBoundType = range.hasUpperBound() ? range.upperBoundType() : null;
        _requestedAttributes = attr;

    }

    /**
     * Returns the UUID identifying this request.
     */
    public UUID getUUID() {
        return _uuid;
    }

    /**
     * Returns the optional range bracketing the result.
     */
    public Range<Integer> getRange() {
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

    /**
     * True if and only if the reply should include file meta data.
     */
    public Set<FileAttribute> getRequestedAttributes() {
        return _requestedAttributes;
    }


    /**
     * Adds an entry to the entry list.
     */
    public void addEntry(String name, FileAttributes attr) {
        _entries.add(new DirectoryEntry(name, attr));
    }

    /**
     * Sets a new entry list.
     */
    public void setEntries(Collection<DirectoryEntry> entries) {
        _entries = entries;
    }

    /**
     * Returns the entry list.
     */
    public Collection<DirectoryEntry> getEntries() {
        return _entries;
    }

    /**
     * Clears the entry list.
     */
    public void clear() {
        _entries.clear();
    }

    @Override
    public void setSucceeded() {
        super.setSucceeded();
        _isFinal = true;
    }

    public void setSucceeded(int messageCount) {
        setSucceeded();
        _messageCount = messageCount;
    }


    public boolean isFinal() {
        return _isFinal;
    }


    public int getMessageCount() {
        return _messageCount;
    }

    public void setMessageCount(int messageCount) {
        _messageCount = messageCount;
    }


    @Override
    public boolean invalidates(Message message) {
        return false;
    }


}