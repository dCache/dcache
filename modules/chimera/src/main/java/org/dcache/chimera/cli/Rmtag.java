package org.dcache.chimera.cli;

import org.dcache.chimera.FileSystemProvider;
import org.dcache.chimera.FsFactory;
import org.dcache.chimera.FsInode;

/**
 *  Remove a tag from a directory
 */
public class Rmtag
{

    public static void main(String[] args) throws Exception
    {
        if (args.length != FsFactory.ARGC +2) {
            System.err.println(
                    "Usage : " + Rmtag.class.getName() + " " + FsFactory.USAGE
                    + " <path> <tag>");
            System.exit(4);
        }

        String tag = args[FsFactory.ARGC + 1];

        try (FileSystemProvider fs = FsFactory.createFileSystem(args)) {
            FsInode inode = fs.path2inode(args[FsFactory.ARGC]);
            fs.removeTag(inode, tag);
        }
    }
}
