package org.dcache.chimera.cli;

import java.util.List;

import org.dcache.acl.ACE;
import org.dcache.acl.enums.RsType;
import org.dcache.chimera.FileSystemProvider;
import org.dcache.chimera.FsFactory;
import org.dcache.chimera.FsInode;

/**
 *
 * @sine 1.9.13
 */
public class Getfacl {

    public static void main(String[] args) throws Exception {

        if (args.length < FsFactory.ARGC + 1) {
            System.err.println(
                    "Usage : " + Getfacl.class.getName() + " " + FsFactory.USAGE
                    + " <chimera path>");
            System.exit(4);
        }

        try (FileSystemProvider fs = FsFactory.createFileSystem(args)) {
            FsInode inode = fs.path2inode(args[FsFactory.ARGC]);
            List<ACE> acl = fs.getACL(inode);
            for (ACE ace : acl) {
                System.out.println(ace.toExtraFormat(inode
                        .isDirectory() ? RsType.DIR : RsType.FILE));
            }
        }

    }
}
