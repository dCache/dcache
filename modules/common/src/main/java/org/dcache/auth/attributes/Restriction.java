/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2015 Deutsches Elektronen-Synchrotron
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.dcache.auth.attributes;

import java.io.Serializable;

import diskCacheV111.util.FsPath;

/**
 * A Restriction provides an overlay authorisation layer where some actions of
 * that a user would normally enjoy are denied; i.e., a Restriction may only
 * reduce what a user is able to achieve should the user's requests be
 * considered separately from any Restriction.
 * <p>
 * The main use-case is to allow delegation with reduced authorisation.
 * Delegation is where one agent (that is authorised to use dCache) somehow
 * allows another agent (that is otherwise not allowed to use dCache) to access
 * dCache according to the first agent's authorisation.  This is commonly
 * referred to as the second agent "acting on behalf of" the first agent.
 * Delegation is often achieved by the first agent providing some credential to
 * the second agent.  Ideally, the credential passed to the second agent is
 * limited as much as possible, to reduce misuse should the second agent be
 * untrustworthy or the credential is stolen.  When logging in, the second
 * agent's delegated credential would attract a Restriction that limits what is
 * authorised.
 * <p>
 * Here is a mapping between common operations and the corresponding Restriction
 * checks:
 * <dl>
 *     <dt>Change current directory</dt>
 *     <dd>
 *         Requires {@code Activity.READ_METADATA} on the new path.
 *     </dd>
 *
 *     <dt>List contents of a directory</dt>
 *     <dd>
 *         Requires {@code Activity.LIST} on the directory path.  Each child
 *         of the directory that does not have {@code Activity.READ_METADATA}
 *         is excluded from the list.
 *     </dd>
 *
 *     <dt>Read information about a file or directory</dt>
 *     <dd>
 *         Requires {@code Activity.READ_METADATA} on the path.
 *     </dd>
 *
 *     <dt>Write a new file</dt>
 *     <dd>
 *         Requires {@code Activity.UPLOAD} on the new file's path.
 *     </dd>
 *
 *     <dt>Delete a file or directory</dt>
 *     <dd>
 *         Requires {@code Activity.DELETE} on the path.
 *     </dd>
 *
 *     <dt>Rename or move a file</dt>
 *     <dd>
 *         Requires {@code Activity.MANAGE} on source's and target's parent
 *         directories.  If the move would overwrite an existing file then
 *         {@code Activity.DELETE} is also needed on the target path.
 *     </dd>
 *
 *     <dt>Create a symbolic link</dt>
 *     <dd>
 *         Requires {@code Activity.MANAGE} on the symbolic link's parent
 *         directory.
 *     </dd>
 *
 *     <dt>Creating an internal copy of a file</dt>
 *     <dd>
 *         Requires {@code Activity.DOWNLOAD} on source file and
 *         {@code Activity.UPLOAD} on the target directory.
 *     </dd>
 * </dl>
 * <p>
 * Restrictions should be written with a "No Islands" rule in mind.
 * Specifically, all Restrictions should be written such that, if a path has no
 * restriction for some activity then the parent path has no restriction for
 * {@code Activity.READ_METADATA}.  The consequence is that, when checking
 * permissions, it is safe to check only the longest (or most specific) path
 * against the user's activity.
 * <p>
 * Restrictions can form a subsumption hierarchy.  A restriction A is said to
 * subsume a restriction B if a denied operations in A are also denied by B.
 * Intuitively, B subsumes A if B is at least as strict as A.
 */
public interface Restriction extends LoginAttribute, Serializable
{
    /**
     * Discover if the user is restricted for this activity and path.
     * @param activity What the user is attempting.
     * @param path absolute path within dCache of the namespace entry affected.
     * @return true if the user is not allowed this activity.
     */
    boolean isRestricted(Activity activity, FsPath path);

    /**
     * An optimised version of isRestricted.  A restriction must respond as
     * if {@literal isRestricted(activity, new FsPath(directory).add(child));}
     * were called, but the method may be able to avoid creating a new FsPath
     * object.
     * @param activity What the user is attempting.
     * @param directory The directory containing the target
     * @param child The name of the target object within directory.
     * @return true if the user is not allowed this activity.
     */
    boolean isRestricted(Activity activity, FsPath directory, String child);

    /**
     * Return true iff there is a child of the supplied path whether the
     * activity is not restricted.
     */
    boolean hasUnrestrictedChild(Activity activity, FsPath parent);

    /**
     * Whether another object is an equivalent restriction.
     * @param other The object to compare
     * @return true iff {@literal other} implements {@literal Restriction}
     * and {@code #isRestricted} and {@literal other.isRestricted} return the
     * same value for all (activity,path) pairs.
     */
    @Override
    boolean equals(Object other);

    /**
     * Check whether {@literal other} denies all operations that this
     * restriction denies.  This is a transitive relationship.  Note that if
     * {@literal A.isSubsumedBy(B)} and {@literal B.isSubsumedBy(A)} then
     * A and B are equivalence.  Two restrictions that are equal are always
     * equivalent; however, there may be pairs of equivalent restrictions that
     * are not equal.  If a class cannot determine whether it is subsumed by
     * some other class then it should return false.
     * @param other The Restriction to compare.
     * @return true if {@code other#isRestricted} returns true for all
     * (activity,path) pairs that {@code #isRestricted} returns true.
     */
    boolean isSubsumedBy(Restriction other);

    /**
     * Provide a short, single-line description of this restriction.
     */
    String toString();
}
