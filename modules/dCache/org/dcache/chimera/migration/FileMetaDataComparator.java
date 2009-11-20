package org.dcache.chimera.migration;

import java.io.PrintStream;

import org.dcache.auth.Subjects;

import diskCacheV111.namespace.NameSpaceProvider;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.FileMetaData;
import diskCacheV111.util.PnfsId;

/**
 * The FileMetaDataComparator class provides a means for validating a
 * PNFS-ID. When created, the FileMetaDataComparator accepts two
 * NameSpaceProvider objects. For each PnfsId that is to be validated, it
 * compares the FileMetaData objects from the two NameSpaceProvider objects.
 *
 * @author Paul Millar <paul.millar@desy.de>
 */
public class FileMetaDataComparator implements PnfsIdValidator {

    final private PrintStream _pw;
    final private NameSpaceProvider _nsp1;
    final private NameSpaceProvider _nsp2;
    final private String _nsp1Name;
    final private String _nsp2Name;

    /**
     * Compare FileMetaData from two NameSpaceProvider objects. No error
     * output is emitted.
     */
    public FileMetaDataComparator( NameSpaceProvider nsp1,
                                   NameSpaceProvider nsp2) {
        this( null, nsp1, nsp1.getClass().getSimpleName(), nsp2, nsp2
                .getClass().getSimpleName());
    }

    /**
     * Compare FileMetaData from two NameSpaceProvider objects. Errors are
     * described by writing information to the supplied PrintStream.
     */
    public FileMetaDataComparator( PrintStream pw, NameSpaceProvider nsp1,
                                   NameSpaceProvider nsp2) {
        this( pw, nsp1, nsp1.getClass().getSimpleName(), nsp2, nsp2.getClass()
                .getSimpleName());
    }

    /**
     * Compare FileMetaData from two NameSpaceProvider objects. Errors are
     * described by writing information to the supplied PrintStream. The
     * NameSpaceProviders nsp1 and nsp2 are identified by the labels name1
     * and name2 respectively.
     */
    public FileMetaDataComparator( PrintStream pw, NameSpaceProvider nsp1,
                                   String name1, NameSpaceProvider nsp2,
                                   String name2) {
        _pw = pw;
        _nsp1 = nsp1;
        _nsp1Name = name1;
        _nsp2 = nsp2;
        _nsp2Name = name2;
    }

    @Override
    public boolean isOK( PnfsId pnfsId) {
        FileMetaData fmd1, fmd2;

        try {
            fmd1 = _nsp1.getFileMetaData( Subjects.ROOT, pnfsId);
        } catch (IllegalArgumentException e) {

            if( _pw != null)
                _pw.println( "Failed to obtain FileMetaData from " + _nsp1Name +
                             ": " + e.getMessage());
            return false;

        } catch (CacheException e) {
            if( _pw != null)
                _pw.println( "Failed to obtain FileMetaData from " + _nsp1Name +
                             ": " + e.getMessage());
            return false;
        }

        try {
            fmd2 = _nsp2.getFileMetaData( Subjects.ROOT, pnfsId);
        } catch (IllegalArgumentException e) {

            if( _pw != null)
                _pw.println( "Failed to obtain FileMetaData from " + _nsp2Name +
                             ": " + e.getMessage());
            return false;

        } catch (CacheException e) {
            if( _pw != null)
                _pw.println( "Failed to obtain FileMetaData from " + _nsp2Name +
                             ": " + e.getMessage());
            return false;
        }

        if( fmd1 == null && fmd2 == null)
            return true;

        if( fmd1 == null) {
            if( _pw != null)
                _pw.println( "ID " + pnfsId + " has null FileMetaData from " +
                             _nsp1Name);
            return false;
        }

        if( fmd1.equals( fmd2))
            return true;

        if( _pw != null) {
            _pw.println( "ID " + pnfsId +
                         " failed consistency check for File Metadata.");
            _pw.println( "\t" + _nsp1Name + ":");
            _pw.println( "\t\t" + fmd1);
            _pw.println( "\t" + _nsp2Name + ":");
            _pw.println( "\t\t" + fmd2);

            emitDifferences( fmd1, fmd2);
        }

        return false;
    }

    /**
     * Emit an explanation of how two FileMetaData objects are different.
     */
    private void emitDifferences( FileMetaData fmd1, FileMetaData fmd2) {
        /**
         * Here we explain the differences between the two FileMetaData
         * objects. Sometimes it isn't obvious.
         */
        _pw.println( "\tDifferences:");
        if( fmd1.getGid() != fmd2.getGid())
            _pw.println( "\t\tgid: " + fmd1.getGid() + " != " + fmd2.getGid());

        if( fmd1.getUid() != fmd2.getUid())
            _pw.println( "\t\tuid: " + fmd1.getUid() + " != " + fmd2.getUid());

        if( fmd1.isDirectory() != fmd2.isDirectory())
            _pw.println( "\t\tdirectory: " + fmd1.isDirectory() + " != " +
                         fmd2.isDirectory());

        if( fmd1.isSymbolicLink() != fmd2.isSymbolicLink())
            _pw.println( "\t\tsym-link: " + fmd1.isSymbolicLink() + " != " +
                         fmd2.isSymbolicLink());

        if( fmd1.isRegularFile() != fmd2.isRegularFile())
            _pw.println( "\t\tregular file: " + fmd1.isRegularFile() + " != " +
                         fmd2.isRegularFile());

        if( fmd1.getFileSize() != fmd2.getFileSize())
            _pw.println( "\t\tsize: " + fmd1.getFileSize() + " != " +
                         fmd2.getFileSize());

        if( !fmd1.getUserPermissions().equals( fmd2.getUserPermissions()))
            _pw.println( "\t\tuser permissions: " + fmd1.getUserPermissions() +
                         " != " + fmd2.getUserPermissions());

        if( !fmd1.getGroupPermissions().equals( fmd2.getGroupPermissions()))
            _pw.println( "\t\tgroup permissions: " + fmd1.getGroupPermissions() +
                         " != " + fmd2.getGroupPermissions());

        if( !fmd1.getWorldPermissions().equals( fmd2.getWorldPermissions()))
            _pw.println( "\t\tworld permissions: " + fmd1.getWorldPermissions() +
                         " != " + fmd2.getWorldPermissions());

        if( fmd1.getCreationTime() != fmd2.getCreationTime())
            _pw.println( "\t\tcreation-time: " + fmd1.getCreationTime() + " != " +
                         fmd2.getCreationTime());

        if( fmd1.getLastAccessedTime() != fmd2.getLastAccessedTime())
            _pw.println( "\t\tlast-accessed: " + fmd1.getLastAccessedTime() +
                         " != " + fmd2.getLastAccessedTime());

        if( fmd1.getLastModifiedTime() != fmd2.getLastModifiedTime())
            _pw.println( "\t\tlast-modified: " + fmd1.getLastModifiedTime() +
                         " != " + fmd2.getLastModifiedTime());
    }

}
