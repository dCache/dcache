package org.dcache.chimera.cli;

import java.util.ArrayList;
import java.util.List;

import org.dcache.acl.ACE;
import org.dcache.acl.parser.ACEParser;
import org.dcache.chimera.FileSystemProvider;
import org.dcache.chimera.FsFactory;
import org.dcache.chimera.FsInode;

/**
 *
 * @sine 1.9.13
 */
public class Setfacl {

    public static void main(String[] args) throws Exception {

        if (args.length < FsFactory.ARGC + 2) {
            System.err.println(
                    "Usage : " + Setfacl.class.getName() + " " + FsFactory.USAGE
                    + " <chimera path> <ACE> [ <ACE> ...]");
            System.exit(4);
        }

        try (FileSystemProvider fs = FsFactory.createFileSystem(args)) {
            List<ACE> acl = new ArrayList<>();
            for (int i = FsFactory.ARGC + 1; i < args.length; i++) {
                acl.add(ACEParser.parse(args[i]));
            }
            FsInode inode = fs.path2inode(args[FsFactory.ARGC]);
            fs.setACL(inode, acl);
        }

    }
}

