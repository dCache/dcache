package org.dcache.chimera.cli;

import org.dcache.chimera.ChimeraFsException;
import org.dcache.chimera.FileNotFoundHimeraFsException;
import org.dcache.chimera.FileSystemProvider;
import org.dcache.chimera.FsFactory;
import org.dcache.chimera.FsInode;

import static com.google.common.io.ByteStreams.toByteArray;

/**
 * Overwrite contents of a file with user-supplied data.
 */
public class Writedata {

    public static void main(String[] args) throws Exception
    {

        int programArgc = args.length - FsFactory.ARGC;

        if( programArgc < 1 || programArgc > 2) {
            System.err.println(
                    "Usage : " + Writedata.class.getName() + " " + FsFactory.USAGE
                    + " <chimera path> [<data>]");
            System.exit(4);
        }

        try (FileSystemProvider fs = FsFactory.createFileSystem(args)) {
            byte[] data = programArgc == 1 ? toByteArray(System.in) :
                    newLineTerminated(args[FsFactory.ARGC + 1]).getBytes();

            writeDataIntoFile(fs, args[FsFactory.ARGC], data);
        }

    }

    private static void writeDataIntoFile(FileSystemProvider fs, String filePath, byte[] data)
            throws ChimeraFsException
    {
        try {
            fs.stat(filePath);
        } catch (FileNotFoundHimeraFsException fnf) {
            fs.createFile(filePath);
        }

        FsInode inode = fs.path2inode(filePath);

        fs.setInodeIo( inode, true);
        inode.write(0, data, 0, data.length);
    }

    private static String newLineTerminated(String unknown)
    {
        return unknown.endsWith("\n") ? unknown : unknown + "\n";
    }
}
