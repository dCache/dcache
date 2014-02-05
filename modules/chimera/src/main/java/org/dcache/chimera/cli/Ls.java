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
package org.dcache.chimera.cli;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.dcache.chimera.DirectoryStreamB;
import org.dcache.chimera.FileSystemProvider;
import org.dcache.chimera.FsFactory;
import org.dcache.chimera.FsInode;
import org.dcache.chimera.HimeraDirectoryEntry;
import org.dcache.chimera.UnixPermission;
import org.dcache.chimera.posix.Stat;

import static com.google.common.base.Strings.padStart;

public class Ls
{
    /* The block size is purely nominal; we use 1k here as historically
     * filesystems have used a 1k block size. */
    private static final int BLOCK_SIZE = 1024;

    private static final  int[] INT_SIZE_TABLE = {9, 99, 999, 9999, 99999,
        999999, 9999999, 99999999, 999999999, Integer.MAX_VALUE};

    private static final DateFormat WITH_YEAR =
            new SimpleDateFormat("MMM dd  yyyy");

    private static final DateFormat WITHOUT_YEAR =
            new SimpleDateFormat("MMM dd HH:mm");

    private static int nlinkWidth = 0;
    private static int uidWidth = 0;
    private static int gidWidth = 0;
    private static int sizeWidth = 0;

    private static final long sixMonthsInPast = sixMonthsInPast();
    private static final long oneHourInFuture = oneHourInFuture();

    public static void main(String[] args) throws Exception
    {

        if (args.length != FsFactory.ARGC + 1) {
            System.err.println("Usage : " + Ls.class.getName() + " " + FsFactory.USAGE
                    + " <chimera path>");
            System.exit(4);
        }

        List<HimeraDirectoryEntry> entries = new LinkedList<>();

        long totalBlocks = 0;

        HimeraDirectoryEntry dot = null;
        HimeraDirectoryEntry dotdot = null;

        try (FileSystemProvider fs = FsFactory.createFileSystem(args)) {
            FsInode inode = fs.path2inode(args[FsFactory.ARGC]);

            try (DirectoryStreamB<HimeraDirectoryEntry> dirStream = inode
                    .newDirectoryStream()) {
                for (HimeraDirectoryEntry entry : dirStream) {
                    String name = entry.getName();
                    Stat stat = entry.getStat();

                    if (name.equals(".")) {
                        dot = entry;
                    } else if (name.equals("..")) {
                        dotdot = entry;
                    } else {
                        entries.add(entry);
                    }

                    totalBlocks = updateTotalBlocks(totalBlocks, stat);
                    nlinkWidth = updateMaxWidth(nlinkWidth, stat.getNlink());
                    uidWidth = updateMaxWidth(uidWidth, stat.getUid());
                    gidWidth = updateMaxWidth(gidWidth, stat.getGid());
                    sizeWidth = updateMaxWidth(sizeWidth, stat.getSize());
                }
            }
        }

        System.out.println("total " + totalBlocks);
        printEntry(dot);
        printEntry(dotdot);
        for(HimeraDirectoryEntry entry : entries) {
            printEntry(entry);
        }
    }

    private static void printEntry(HimeraDirectoryEntry entry)
    {
        if(entry != null) {
            Stat stat = entry.getStat();
            String s = String.format("%s %s %s %s %s %s %s",
                        permissionsFor(stat),
                        pad(stat.getNlink(), nlinkWidth),
                        pad(stat.getUid(), uidWidth),
                        pad(stat.getGid(), gidWidth),
                        pad(stat.getSize(), sizeWidth),
                        dateOf(stat.getMTime()),
                        entry.getName());

            System.out.println(s);
        }
    }

    // For files with a time that is more than 6 months old or more than 1
    // hour into the future, the timestamp contains the year instead of the
    // time of day.
    private static String dateOf(long time)
    {
        Date d = new Date(time);

        if(time < sixMonthsInPast || time > oneHourInFuture) {
            return WITH_YEAR.format(d);
        } else {
            return WITHOUT_YEAR.format(d);
        }
    }

    private static long sixMonthsInPast()
    {
        Calendar calendar = Calendar.getInstance();
        calendar.add(Calendar.MONTH, -6);
        return calendar.getTimeInMillis();
    }

    private static long oneHourInFuture()
    {
        return System.currentTimeMillis() + TimeUnit.HOURS.toMillis(1);
    }

    private static String pad(int value, int width)
    {
        String str = String.valueOf(value);
        return padStart(str, width, ' ');
    }

    private static String pad(long value, int width)
    {
        String str = String.valueOf(value);
        return padStart(str, width, ' ');
    }

    private static long updateTotalBlocks(long total, Stat stat)
    {
        // calculate number of blocks, but rounding up
        long nBlocks = 1 + (stat.getSize() -1)/ BLOCK_SIZE;
        return total + nBlocks;
    }

    private static int updateMaxWidth(int max, int value)
    {
        int width = widthOf(value);
        return width > max ? width : max;
    }

    private static int updateMaxWidth(int max, long value)
    {
        int width = widthOf(value);
        return width > max ? width : max;
    }

    private static String permissionsFor(Stat stat)
    {
        return new UnixPermission(stat.getMode()).toString();
    }


    // Requires positive x
    private static int widthOf(int x)
    {
        for (int i=0; ; i++) {
            if (x <= INT_SIZE_TABLE[i]) {
                return i+1;
            }
        }
    }

    // Requires positive x
    private static int widthOf(long x)
    {
        if(x <= Integer.MAX_VALUE) {
            return widthOf((int) x);
        }

        // x is more than 0x7fffffff or 2147483647

        if (x < 1000000000000L) { // from 10 to 12 digits
            if (x < 10000000000L) {
                return 10;
            } else {
                return x < 100000000000L ? 11 : 12;
            }
        } else { // 13 or more digits
            if (x < 10000000000000000L) {
                if (x < 100000000000000L) {
                    return x < 10000000000000L ? 13 : 14;
                } else {
                    return x < 1000000000000000L ? 15 : 16;
                }
            } else {
                if (x < 1000000000000000000L) {
                    return x < 100000000000000000L ? 17 : 18;
                } else {
                    return 19;
                }
            }
        }
    }
}
