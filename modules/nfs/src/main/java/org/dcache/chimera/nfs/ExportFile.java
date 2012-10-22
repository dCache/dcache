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

import com.google.common.base.Splitter;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;
import java.net.URL;

import org.dcache.chimera.nfs.ExportClient.IO;
import org.dcache.chimera.nfs.ExportClient.Root;


public class ExportFile {

    /**
     * The root node of pseudo fs.
     */
    private volatile PseudoFsNode _pseudoFS = new PseudoFsNode(null);
    private final URL _exportFile;

    public ExportFile(File file) throws IOException
    {
        this(file.toURL());
    }

    public ExportFile(URL file) throws IOException
    {
        _exportFile = file;
        _pseudoFS = scanExportFile(file);
    }

    public List<String> getExports() {
        List<String> out = new ArrayList<>();
        PseudoFsNode pseudoFsRoot = _pseudoFS;
        walk(out, pseudoFsRoot, null);
        return out;
    }

    private void walk(List<String> out, PseudoFsNode node, String path) {
        if(node.isMountPoint()) {
            out.add(path == null ? "/" : path);
        }

        if(node.isLeaf()) {
            return;
        }

        for(PseudoFsNode next: node.getChildren()) {
            walk(out, next, (path == null ? "" : path) + "/" + next.getName());
        }
    }

    private PseudoFsNode scanExportFile(URL exportFile) throws IOException
    {
        BufferedReader br = new BufferedReader(new InputStreamReader(exportFile.openStream()));
        PseudoFsNode pseudoFsRoot = new PseudoFsNode(null);

        String line;
        try {
            int lineCount = 0;
            while ((line = br.readLine()) != null) {

                ++lineCount;

                line = line.trim();
                if (line.length() == 0) {
                    continue;
                }

                if (line.charAt(0) == '#') {
                    continue;
                }

                FsExport  export;
                StringTokenizer st = new StringTokenizer(line);
                String path = st.nextToken();
                String referral = null;

                if( st.hasMoreTokens() ) {
                    List<ExportClient> clients = new ArrayList<>();
                    while(st.hasMoreTokens() ) {

                        String hostAndOptions = st.nextToken();
                        StringTokenizer optionsTokenizer = new StringTokenizer(hostAndOptions, "(),");

                        String host = optionsTokenizer.nextToken();
                        Root isTrusted = ExportClient.Root.NOTTRUSTED;
                        IO rw = ExportClient.IO.RO;
                        while(optionsTokenizer.hasMoreTokens()) {

                            String option = optionsTokenizer.nextToken();
                            if( option.equals("rw") ) {
                                rw = ExportClient.IO.RW;
                                continue;
                            }

                            if( option.equals("no_root_squash") ) {
                                isTrusted = ExportClient.Root.TRUSTED;
                                continue;
                            }

                            if(option.startsWith("refer=")) {
                                referral = option.substring("refer=".length());
                            }
                        }

                        ExportClient client = new ExportClient(host,isTrusted, rw);
                        clients.add(client);
                    }
                    export  = new FsExport(path, clients, referral);
                }else{
                    ExportClient everyOne = new ExportClient("*",ExportClient.Root.NOTTRUSTED, ExportClient.IO.RO );

                    List<ExportClient> clients = new ArrayList<>(1);
                    clients.add(everyOne);
                    export = new FsExport(path, clients, referral );
                }

                pathToPseudoFs(pseudoFsRoot, path, export);
            }
        } finally {
            try {
                br.close();
            } catch (IOException dummy) {
                // ignored
            }
        }
        return pseudoFsRoot;
    }


    public FsExport getExport(String path) {
        PseudoFsNode node = getExportNode(path);
        return node == null? null : node.getExport();
    }

    public PseudoFsNode getExportNode(String path) {

        if (path.equals("/")) {
            return _pseudoFS;
        }

        Splitter splitter = Splitter.on('/').omitEmptyStrings();
        PseudoFsNode rootNode = _pseudoFS;
        PseudoFsNode node = null;

        for (String s : splitter.split(path)) {
            node = rootNode.getNode(s);
            if (node == null) {
                return null;
            }
            rootNode = node;
        }

        return node;
    }

    // FIXME: one trusted client has an access to all tree
    public  boolean isTrusted( java.net.InetAddress client ){


        List<String> exports = getExports();
        for( String path: exports ) {

            FsExport fsExport = getExport(path);
            if( fsExport != null && fsExport.isTrusted(client) ) {
                return true;
            }

        }

        return false;

    }

    /**
     * Parse export path into a chain of nodes.
     * Each node represents a directory of the a pseudo file system.
     * @param path
     * @param e
     */
    private void pathToPseudoFs(PseudoFsNode parent, String path, FsExport e) {

        Splitter splitter = Splitter.on('/').omitEmptyStrings();
        for (String s : splitter.split(path)) {
            PseudoFsNode node = parent.getNode(s);
            if (node == null) {
                node = new PseudoFsNode(s);
                parent.addChild(node);
            }
            parent = node;
        }
        parent.setExport(e);
    }

    public PseudoFsNode getPseuFsRoot() {
        return _pseudoFS;
    }

    public void rescan() throws IOException {
        _pseudoFS = scanExportFile(_exportFile);
    }
}
