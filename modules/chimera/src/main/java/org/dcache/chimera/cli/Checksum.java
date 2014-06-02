package org.dcache.chimera.cli;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;

import org.dcache.chimera.ChimeraFsException;
import org.dcache.chimera.FileSystemProvider;
import org.dcache.chimera.FsFactory;
import org.dcache.chimera.FsInode;
import org.dcache.util.ChecksumType;

import static com.google.common.base.Preconditions.checkArgument;

public class Checksum
{
    private static final ImmutableMap<String,Class<? extends Command>> COMMANDS =
       new ImmutableMap.Builder<String,Class<? extends Command>>()
           .put("help", HelpCommand.class)
           .put("list", ListCommand.class)
           .put("get", GetCommand.class)
           .put("add", AddCommand.class)
           .put("delete", DeleteCommand.class)
           .build();

    private static final Class<? extends Command> DEFAULT_COMMAND =
            HelpCommand.class;

    public static void main(String[] args) throws Exception
    {
        if (args.length < FsFactory.ARGC + 1) {
            System.err.println(
                    "Usage : " + Checksum.class.getName() + " " + FsFactory.USAGE
                    + " <chimera path> [<cmd> [<args>]]");
            System.exit(4);
        }

        try (Command command = buildCommand(args)) {
            command.run();
        }
    }

    private static Command buildCommand(String[] args) throws Exception
    {
        Class<? extends Command> type = null;

        if (args.length > FsFactory.ARGC + 1) {
            String name = args[FsFactory.ARGC + 1];
            type = COMMANDS.get(name.toLowerCase());
        }

        if (type == null) {
            type = DEFAULT_COMMAND;
        }

        Command command = type.newInstance();

        command.init(args);

        return command;
    }

    public static abstract class Command implements Closeable
    {
        protected FileSystemProvider _fs;
        protected String _path;
        protected String[] _args;

        public void init(String args[]) throws Exception
        {
            _fs = FsFactory.createFileSystem(args);
            _path = args [FsFactory.ARGC];
            _args = buildCommandArgs(args);
        }

        public FsInode getFileInode() throws ChimeraFsException
        {
            FsInode inode = _fs.path2inode(_path);

            if (inode.isDirectory() || inode.isLink()) {
                throw new IllegalArgumentException("not a file: " + _path);
            }

            return inode;
        }

        private static String[] buildCommandArgs(String args[])
        {
            int count = args.length - FsFactory.ARGC - 2;

            String[] commandArgs = new String[count < 0 ? 0 : count];

            if (count > 0) {
                System.arraycopy(args, FsFactory.ARGC + 2, commandArgs, 0, count);
            }

            return commandArgs;
        }

        @Override
        public void close() throws IOException
        {
            _fs.close();
        }

        abstract public void run() throws Exception;
    }

    public static class HelpCommand extends Command
    {
        public static final String DESCRIPTION =
                "provides some help (this information)";

        @Override
        public void run()
        {
            System.out.println("Format:");
            System.out.println("    checksum <path> <command> [<arguments>]\n");

            int max = 0;
            for (String name : COMMANDS.keySet()) {
                if (name.length() > max) {
                    max = name.length();
                }
            }

            System.out.println("Valid commands are:");

            for (Map.Entry<String,Class<? extends Command>> entry :
                    COMMANDS.entrySet()) {
                String name = entry.getKey();
                String description = getDescription(entry.getValue());
                System.out.println("    " + Strings.padEnd(name, max, ' ') +
                        " - " + description);
            }
        }

        public String getDescription(Class<? extends Command> command)
        {
            try {
                return (String) command.getDeclaredField("DESCRIPTION").
                        get(null);
            } catch (IllegalAccessException | SecurityException |
                    NoSuchFieldException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public static class ListCommand extends Command
    {
        public static final String DESCRIPTION = "list all checksums for " +
                "a file";

        @Override
        public void run() throws Exception
        {
            FsInode inode = getFileInode();

            for(org.dcache.util.Checksum checksum : _fs.getInodeChecksums(inode)) {
                    System.out.println(checksum.getType().getName() + ":" + checksum.getValue());
            }
        }
    }

    public static class GetCommand extends Command
    {
        public static final String DESCRIPTION = "prints the value of file's " +
                "checksum for the given algorithm, if known";

        @Override
        public void run() throws Exception
        {
            checkArgument(_args.length == 1, "need precisely one argument " +
                    "after 'get'");

            ChecksumType type = ChecksumType.getChecksumType(_args[0]);

            FsInode inode = getFileInode();

            for(org.dcache.util.Checksum checksum: _fs.getInodeChecksums(inode)) {
                if (checksum.getType() == type) {
                    System.out.println(checksum.getValue());
                    return;
                }
            }

            System.out.println("No checksum of type " + type.getName());
        }
    }

    public static class AddCommand extends Command
    {
        public static final String DESCRIPTION = "updated file to have an " +
                "additional checksum value";

        @Override
        public void run() throws Exception
        {
            checkArgument(_args.length == 2, "need precisely two arguments " +
                    "after 'add'");

            ChecksumType type = ChecksumType.getChecksumType(_args[0]);

            org.dcache.util.Checksum checksum =
                    new org.dcache.util.Checksum(type, _args[1]);

            FsInode inode = getFileInode();

            _fs.setInodeChecksum(inode, type.getType(), checksum.getValue());
        }
    }

    public static class DeleteCommand extends Command
    {
        public static final String DESCRIPTION = "remove the checksum " +
                "generated with the specified algorithm";

        @Override
        public void run() throws Exception
        {
            checkArgument(_args.length == 1, "Need precisely one argument " +
                    "after 'delete', the checksum algorithm");

            ChecksumType type = ChecksumType.getChecksumType(_args[0]);

            FsInode inode = getFileInode();

            _fs.removeInodeChecksum(inode, type.getType());
        }
    }
}
