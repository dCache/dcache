package org.dcache.chimera.cli;

import org.dcache.chimera.FileSystemProvider;
import org.dcache.chimera.FsInode;
import org.dcache.util.ChecksumType;

public class Checksum {

    public static void main(String[] args) throws Exception {

        if (args.length < FsFactory.ARGC + 1) {
            System.err.println(
                    "Usage : " + Checksum.class.getName() + " " + FsFactory.USAGE
                    + " <chimera path>");
            System.exit(4);
        }

        try (FileSystemProvider fs = FsFactory.createFileSystem(args)) {
            FsInode inode = fs.path2inode(args[FsFactory.ARGC]);
            String checksum = fs
                    .getInodeChecksum(inode, ChecksumType.ADLER32.getType());
            if (checksum == null)
                checksum = "N.A.";
            System.out.println(checksum);
        }

    }
}

