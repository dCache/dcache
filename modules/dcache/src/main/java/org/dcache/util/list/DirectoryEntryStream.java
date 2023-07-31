package org.dcache.util.list;

/**
 * Interface that denotes a DirectoryStream of 'virtual' {@link DirectoryEntry}s. Importantly, since
 * the {@link DirectoryEntry}s are virtual, we don't have an IOException on closing.
 */
public interface DirectoryEntryStream extends java.nio.file.DirectoryStream<DirectoryEntry> {

    // We have DirectoryStreams that don't throw IOExceptions.
    // We use this interface to override the signature to avoid handling IOExceptions we don't throw.
    void close();
}
