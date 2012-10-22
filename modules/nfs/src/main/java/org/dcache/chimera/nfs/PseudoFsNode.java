/*
 * This library is free software; you can redistribute it and/or modify
 * it under the terms of the GNU Library General Public License as
 * published by the Free Software Foundation; either version 2 of the
 * License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Library General Public License for more details.
 *
 * You should have received a copy of the GNU Library General Public
 * License along with this program (see the file COPYING.LIB for more
 * details); if not, write to the Free Software Foundation, Inc.,
 * 675 Mass Ave, Cambridge, MA 02139, USA.
 */
package org.dcache.chimera.nfs;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * A Graph like structure to represent file system like tree.
 * Each node can point to one or more other nodes (subdirectories).
 * Node without any subdirectory called leaf. There are two data object
 * attached to each node - it's name and, optionally, export information.
 * @author tigran
 */
public class PseudoFsNode {

    private final String _name;
    private FsExport _export;
    private Map<String, PseudoFsNode> _children = new HashMap<>();

    public PseudoFsNode(String name) {
        this._name = name;
    }

    public Collection<PseudoFsNode> getChildren() {
        return _children.values();
    }

    public PseudoFsNode addChild(PseudoFsNode child) {
        _children.put(child.getName(), child);
        return child;
    }

    public String getName() {
        return _name;
    }

    public boolean isMountPoint() {
        return _export != null;
    }

    public boolean isLeaf() {
        return _children.isEmpty();
    }

    public PseudoFsNode getNode(String t) {
        return _children.get(t);
    }

    public FsExport getExport() {
        return _export;
    }

    public void setExport(FsExport e) {
        _export = e;
    }

    @Override
    public String toString() {
        if(_name == null) {
            return "/";
        }

        StringBuilder sb = new StringBuilder();
        sb.append("/").append(_name);
        return sb.toString();
    }
}
