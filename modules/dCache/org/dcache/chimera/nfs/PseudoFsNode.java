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
import java.util.Collections;
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

    private final String data;
    private FsExport export;

    public PseudoFsNode(String data) {
        this.data = data;
    }
    private Map<String, PseudoFsNode> children;

    public Collection<PseudoFsNode> getChildren() {
        if (children == null) {
            return Collections.emptyList();
        }
        return children.values();
    }

    public PseudoFsNode addChild(PseudoFsNode child) {
        if (children == null) {
            children = new HashMap<String, PseudoFsNode>();
        }
        children.put(child.getData(), child);
        return child;
    }

    public String getData() {
        return data;
    }

    public boolean isMountPoint() {
        return export != null;
    }

    public boolean isLeaf() {
        return children == null;
    }

    public PseudoFsNode getNode(String t) {
        if (children == null) {
            return null;
        }
        return children.get(t);
    }

    public FsExport getExport() {
        return export;
    }

    public void addExport(FsExport e) {
        export = e;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        if (data != null) {
            sb.append("/").append(data.toString());
        }
        return sb.toString();
    }
}
