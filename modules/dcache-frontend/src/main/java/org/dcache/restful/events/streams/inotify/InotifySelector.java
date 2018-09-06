/*
 * dCache - http://www.dcache.org/
 *
 * Copyright (C) 2018 Deutsches Elektronen-Synchrotron
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
package org.dcache.restful.events.streams.inotify;

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonSetter;

import java.util.EnumSet;
import java.util.Optional;
import java.util.Set;

import diskCacheV111.util.FsPath;

import org.dcache.restful.events.spi.SelectionResult;

/**
 * The desired selection of events, as supplied by the client using a JSON
 * representation.  This is equivalent to the inotify_add_watch(2) arguments.
 */
@JsonInclude(value = JsonInclude.Include.NON_NULL)
class InotifySelector
{
    public static final EnumSet<AddWatchFlag> UNSUPPORTED_FLAGS = EnumSet.of(
            AddWatchFlag.IN_DONT_FOLLOW, AddWatchFlag.IN_EXCL_UNLINK);

    @JsonIgnore
    private Set<AddWatchFlag> flags = AddWatchFlag.ALL_EVENTS;
    private Set<AddWatchFlag> suppliedFlags;
    private String path;
    @JsonIgnore
    private FsPath fsPath;

    public void setPath(String path)
    {
        this.path = path;
        fsPath = FsPath.create(path);
    }

    @JsonSetter("flags")
    public void setSuppliedFlags(EnumSet<AddWatchFlag> suppliedFlags)
    {
        this.suppliedFlags = EnumSet.copyOf(suppliedFlags);
        flags = EnumSet.noneOf(AddWatchFlag.class);
        for (AddWatchFlag suppliedFlag : suppliedFlags) {
            switch (suppliedFlag) {
            case IN_ALL_EVENTS:
                flags.addAll(AddWatchFlag.ALL_EVENTS);
                break;
            case IN_CLOSE:
                flags.add(AddWatchFlag.IN_CLOSE_WRITE);
                flags.add(AddWatchFlag.IN_CLOSE_NOWRITE);
                break;
            case IN_MOVE:
                flags.add(AddWatchFlag.IN_MOVED_FROM);
                flags.add(AddWatchFlag.IN_MOVED_TO);
                break;
            default:
                flags.add(suppliedFlag);
            }
        }
    }

    @JsonGetter("flags")
    public Set<AddWatchFlag> getSuppliedFlags()
    {
        return suppliedFlags;
    }

    public Set<AddWatchFlag> flags()
    {
        return flags;
    }

    public String getPath()
    {
        return path;
    }

    public FsPath getFsPath()
    {
        return fsPath;
    }

    public Optional<SelectionResult> validationError()
    {
        if (path == null) {
            return Optional.of(SelectionResult.badSelector("Missing path"));
        }

        EnumSet<AddWatchFlag> unsupported = EnumSet.copyOf(flags);
        unsupported.retainAll(UNSUPPORTED_FLAGS);
        if (!unsupported.isEmpty()) {
            return Optional.of(SelectionResult.badSelector("Unsupported flags: "
                    + unsupported));
        }

        return Optional.empty();
    }
}
