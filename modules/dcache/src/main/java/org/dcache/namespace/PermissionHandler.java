package org.dcache.namespace;

import javax.security.auth.Subject;

import java.util.Set;
import javax.annotation.Nonnull;

import org.dcache.acl.enums.AccessType;
import org.dcache.vehicles.FileAttributes;

/**
 * A PermissionHandler makes policy decisions for file access. It is
 * typically used by NameSpaceProvider objects.
 *
 * When referring to a parent directory, null is used for the
 * non-existing parent of the root directory.
 */
public interface PermissionHandler
{
    /**
     * Returns the set of attributes required to make policy
     * decisions. When calling any of the other methods, one or more
     * FileAttributes objects most be provided containing the
     * attributes specified by the set returned by the
     * getRequiredAttributes method.
     */
    Set<FileAttribute> getRequiredAttributes();

    /**
     * checks whether the user can read file
     *
     * @param subject
     *            identifies the subject that is trying to access a resource
     * @param attr
     *            the attributes of the file to read
     *
     * @return Returns the access type granted
     */
    @Nonnull
    AccessType canReadFile(Subject subject, FileAttributes attr);

    /**
     * checks whether the user can write file
     *
     * @param subject
     *            identifies the subject that is trying to access a resource
     * @param attr
     *            the attributes of the file to write
     *
     * @return Returns the access type granted
     */
    @Nonnull
    AccessType canWriteFile(Subject subject, FileAttributes attr);


    /**
     * checks whether the user can create a sub-directory in a directory
     *
     * @param subject
     *            identifies the subject that is trying to access a resource
     * @param parentAttr
     *            the attributes of the directory in which to create a
     *            sub-directory
     *
     * @return Returns the access type granted
     */
    @Nonnull
    AccessType canCreateSubDir(Subject subject, FileAttributes parentAttr);


    /**
     * checks whether the user can create a file in a directory
     *
     * @param subject
     *            identifies the subject that is trying to access a resource
     * @param parentAttr
     *            the attributes of the directory in which to create a
     *            file
     *
     * @return Returns the access type granted
     */
    @Nonnull
    AccessType canCreateFile(Subject subject, FileAttributes parentAttr);

    /**
     * checks whether the user can delete file
     *
     * @param subject
     *            identifies the subject that is trying to access a resource
     * @param parentAttr
     *            Attributes of directory containing the file to delete
     * @param childAttr
     *            Attributes of the file to be deleted
     *
     * @return Returns the access type granted
     */
    @Nonnull
    AccessType canDeleteFile(Subject subject,
                             FileAttributes parentAttr,
                             FileAttributes childAttr);

    /**
     * checks whether the user can delete directory
     *
     * @param subject
     *            identifies the subject that is trying to access a resource
     * @param parentAttr
     *            Attributes of directory containing the directory to delete
     * @param childAttr
     *            Attributes of the directory to be deleted
     *
     * @return Returns the access type granted
     */
    @Nonnull
    AccessType canDeleteDir(Subject subject,
                            FileAttributes parentAttr,
                            FileAttributes childAttr);

    /**
     * checks whether the user can rename a file
     *
     * @param subject
     *            identifies the subject that is trying to access a resource
     * @param existingParentAttr
     *            Attributes of directory containing the file to rename
     * @param newParentAttr
     *            Attributes of the new parent directory
     * @param isDirectory
     *            True if and only if the entry to rename is a directory
     *
     * @return Returns the access type granted
     */
    @Nonnull
    AccessType canRename(Subject subject,
                         FileAttributes existingParentAttr,
                         FileAttributes newParentAttr,
                         boolean isDirectory);

    /**
     * checks whether the user can list directory
     *
     * @param subject
     *            identifies the subject that is trying to access a resource
     * @param attr
     *            Attributes of the directory to list
     *
     * @return Returns the access type granted
     */
    @Nonnull
    AccessType canListDir(Subject subject, FileAttributes attr);

    /**
     * checks whether the user can lookup an entry in a directory
     *
     * @param subject
     *            identifies the subject that is trying to access a resource
     * @param attr
     *            Attributes of the directory in which to lookup an entry
     *
     * @return Returns the access type granted
     */
    @Nonnull
    AccessType canLookup(Subject subject, FileAttributes attr);

    /**
     * checks whether the user can set attributes of a file/directory
     *
     * @param subject
     *            identifies the subject that is trying to access a resource
     * @param attr
     *            Attributes of the file for which to modify an attribute
     * @param attributes
     *            Attributes to modify
     *
     * @return Returns the access type granted
     */
    @Nonnull
    AccessType canSetAttributes(Subject subject,
                                FileAttributes attr,
                                Set<FileAttribute> attributes);

    /**
     * checks whether the user can get attributes of a file/directory
     *
     * @param subject
     *            identifies the subject that is trying to access a resource
     * @param attr
     *            Attributes of the file for which to modify an attribute
     * @param attributes
     *            Attributes to retrieve
     *
     * @return Returns the access type granted
     */
    @Nonnull
    AccessType canGetAttributes(Subject subject,
                                FileAttributes attr,
                                Set<FileAttribute> attributes);
}
