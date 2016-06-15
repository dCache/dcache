package diskCacheV111.util;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.ObjectStreamException;
import java.io.Serializable;
import java.util.Iterator;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Immutable absolute abstract path in the dCache name space.
 *
 * An FsPath represents a path in the dCache name space. It is always
 * absolute, meaning it is rooted at the name space root.
 *
 * Self and parent references are eliminated during construction, which
 * means an FsPath never contains '.' or '..' elements. The elimination
 * is done on the abstract path and not on the actual name space, meaning
 * that /a/b/.. becomes /a even if b is a symbolic link, a file or when
 * it doesn't exist.
 */
public abstract class FsPath implements Serializable
{
    private static final long serialVersionUID = -4852395244086808172L;

    /* An FsPath is implemented as a single linked list from the tail to the
     * root. All operations are implemented recursively and modifying operations
     * return a new FsPath rather than changing the existing FsPath.
     */

    /**
     * FsPath representing the file system root. This is the only instance.
     */
    public static final FsPath ROOT = new Root();

    public static FsPath create(String path)
    {
        checkArgument(path.startsWith("/"));
        return ROOT.resolve(path.substring(1));
    }

    /**
     * Provided for compatibility with type conversion libraries that rely
     * on a static valueOf function. Use {@code create} when creating an FsPath
     * directly.
     */
    public static FsPath valueOf(String path)
    {
        return create(path);
    }

    @Override
    public String toString()
    {
        return appendTo(new StringBuilder()).toString();
    }

    /**
     * Append the path represented by this FsPath to {@code path}.
     *
     * @param path StringBuilder to append to
     * @return The {@code path} StringBuilder
     */
    protected abstract StringBuilder appendTo(StringBuilder path);

    /**
     * A version of {@code appendTo} that terminates {@code path} with a slash.
     *
     * @param path StringBuilder to append to
     * @return The {@code path} StringBuilder
     */
    protected abstract StringBuilder appendTo2(StringBuilder path);

    /**
     * Drops the length of {@code path} elements from this FsPath and
     * returns the remaining prefix. Returns null if {@code path}
     * is longer than this FsPath.
     */
    protected abstract FsPath tryDropLength(FsPath path);

    /**
     * Drops the length of {@code path} elements from this FsPath and
     * returns the remaining prefix.
     *
     * Equivalent to {@code drop(path.length())}.
     */
    protected abstract FsPath dropLength(FsPath path);

    /**
     * Returns true if {@code path} forms a suffix of this FsPath.
     *
     * @param path Path elements in reverse order (child before parent)
     * @return True iff {@code path} forms a suffix of this FsPath.
     */
    protected abstract boolean isSuffix(Iterator<String> path);

    /**
     * Returns true if {@code path} is fully contained within this FsPath.
     *
     * @param path Path elements in reverse order (child before parent).
     * @return True iff {@code path} is fully contained within this FsPath.
     */
    protected abstract boolean contains(List<String> path);

    /**
     * Returns true if {@code path} is fully contained within this FsPath.
     *
     * Path elements in {@code path} are separated by a slash. Empty path
     * elements are stripped.
     *
     * @param path Slash separated path to check for inclusion.
     * @return True iff {@code path} is fully contained within this FsPath.
     */
    public boolean contains(String path)
    {
        return contains(Lists.reverse(Splitter.on("/").omitEmptyStrings().splitToList(path)));
    }

    /**
     * Returns true if this is the path of the file system root, false otherwise.
     *
     * @return True iff this is the path of the file system root.
     */
    public abstract boolean isRoot();

    /**
     * Returns the path of this FsPath's parent.
     *
     * @return The path of this FsPath's parent.
     * @throws IllegalArgumentException if this is a root path
     */
    public abstract FsPath parent();

    /**
     * Returns the name of the file or directory denoted by this FsPath.
     *
     * @return The name of the file or directory denoted by this FsPath.
     */
    public abstract String name();

    /**
     * Returns the number of elements of this FsPath.
     *
     * The length of the path of the file system root is 0.
     *
     * @return The number of elements of this FsPath.
     */
    public abstract int length();

    /**
     * Returns the path obtained by dropping up to {@code n} elements of this FsPath.
     *
     * @return The path obtained by dropping up to {@code n} elements of this FsPath.
     */
    public abstract FsPath drop(int n);

    /**
     * Returns the path obtained by resolving {@code path} relative to this FsPath.
     *
     * If {@code path} is a relative path, the path is effectively appended.
     */
    public FsPath resolve(String path)
    {
        int i = path.lastIndexOf('/');
        switch (i) {
        case -1:
            return resolvePathElement(ROOT, path);
        case 0:
            return ROOT.resolvePathElement(ROOT, path.substring(1));
        default:
            return resolve(path.substring(0, i)).resolvePathElement(ROOT, path.substring(i + 1));
        }
    }

    /**
     * Returns the path obtained by resolving {@code path} relative to this FsPath.
     *
     * Differs from {@code resolve} in that this FsPath is considered the root to resolve
     * {@code path} against. This means that {@code path} is appended to this FsPath even
     * if {@code path} is an absolute path. If {@code path} contains .. elements, these
     * never walk above this FsPath.
     */
    public FsPath chroot(String path)
    {
        int i = path.lastIndexOf('/');
        switch (i) {
        case -1:
            return resolvePathElement(this, path);
        case 0:
            return resolvePathElement(this, path.substring(1));
        default:
            return chroot(path.substring(0, i)).resolvePathElement(this, path.substring(i + 1));
        }
    }

    private FsPath resolvePathElement(FsPath root, String name)
    {
        switch (name) {
        case "":
        case ".":
            return this;
        case "..":
            return (this == root) ? this : parent();
        default:
            return new Child(this, name);
        }
    }

    /**
     * Returns an FsPath representing the path of the named {@code name} of the path
     * represented by this FsPath.
     */
    public FsPath child(String name)
    {
        checkArgument(!name.isEmpty(), "Name must be non-empty.");
        checkArgument(!name.contains("/"), "Name must not contain '/'.");
        checkArgument(!name.equals("."), "Name must not be '.'.");
        checkArgument(!name.equals(".."), "Name must not be '..'.");
        return new Child(this, name);
    }

    /**
     * Returns true if the path of {@code prefix} is a prefix of the path of this FsPath.
     */
    public boolean hasPrefix(FsPath prefix)
    {
        /* The purpose of delta is just to describe how much longer this path is than prefix - the
         * actual path in delta is of no interest. It's just a clever way to implement the prefix
         * in O(n + m) steps.
         */
        FsPath delta = tryDropLength(prefix);
        return delta != null && prefix.equals(dropLength(delta));
    }

    /**
     * Strips {@code prefix} from the beginning of this FsPath and returns the string form of
     * the remaining suffix. The suffix starts with a slash.
     *
     * @throws IllegalArgumentException if {@code prefix} is not a prefix of this FsPath.
     */
    public String stripPrefix(FsPath prefix)
    {
        FsPath delta = dropLength(prefix);
        return appendSuffix(prefix, delta, new StringBuilder()).toString();
    }

    /**
     * Appends the string representation of the suffix of this FsPath with the same length as
     * {@code length} to {@code s}.
     *
     * @param prefix A prefix of this path.
     * @param length An FsPath of the same length as the suffix to append to {@code s}.
     * @param s A StringBuilder to append the path to.
     * @return The {@code path} StringBuilder
     * @throws IllegalArgumentException if {@code prefix} is not a prefix of this FsPath or if the
     *                                  the length of this FsPath is not the sum of the length of
     *                                  {@code prefix} and {@code length}.
     */
    protected abstract StringBuilder appendSuffix(FsPath prefix, FsPath length, StringBuilder s);

    /**
     * A version of {@code appendSuffix} that terminates {@code path} with a slash.
     *
     * @param prefix A prefix of this path.
     * @param length An FsPath of the same length as the suffix to append to {@code s}.
     * @param s A StringBuilder to append the path to.
     * @return The {@code path} StringBuilder
     * @throws IllegalArgumentException if {@code prefix} is not a prefix of this FsPath or if the
     *                                  the length of this FsPath is not the sum of the length of
     *                                  {@code prefix} and {@code length}.
     */
    protected abstract StringBuilder appendSuffix2(FsPath prefix, FsPath length, StringBuilder s);

    /**
     * Helper method to write a path to a stream.
     *
     * @param out Stream to write the path to
     * @param len The length of the suffix of this path being written
     * @throws IOException
     */
    protected abstract void write(ObjectOutput out, int len) throws IOException;

    /**
     * Helper method to read a path from a stream.
     *
     * @param in Stream to read the path from
     * @param len The length of the suffix of this path being read
     * @throws IOException
     */
    protected FsPath read(ObjectInput in, int len) throws IOException
    {
        return (len == 0) ? this : child(in.readUTF()).read(in, len - 1);
    }

    private Object writeReplace()
            throws ObjectStreamException
    {
        return new SerializedPath(this);
    }

    private static final class Root extends FsPath
    {
        @Override
        protected StringBuilder appendTo(StringBuilder path)
        {
            return appendTo2(path);
        }

        @Override
        protected StringBuilder appendTo2(StringBuilder path)
        {
            return path.append('/');
        }

        @Override
        public boolean isRoot()
        {
            return true;
        }

        @Override
        public FsPath parent()
        {
            throw new IllegalStateException("Root does not have a parent");
        }

        @Override
        public int hashCode()
        {
            return 0;
        }

        @Override
        public boolean equals(Object o)
        {
            return (o instanceof Root);
        }

        @Override
        public String name()
        {
            return "/";
        }

        @Override
        public int length()
        {
            return 0;
        }

        @Override
        public FsPath drop(int n)
        {
            checkArgument(n >= 0);
            return this;
        }

        @Override
        protected FsPath tryDropLength(FsPath path)
        {
            return path.isRoot() ? this : null;
        }

        @Override
        protected FsPath dropLength(FsPath path)
        {
            return this;
        }

        @Override
        protected boolean isSuffix(Iterator<String> path)
        {
            return !path.hasNext();
        }

        @Override
        protected boolean contains(List<String> path)
        {
            return path.isEmpty();
        }

        @Override
        protected StringBuilder appendSuffix(FsPath prefix, FsPath length, StringBuilder s)
        {
            return appendSuffix2(prefix, length, s);
        }

        @Override
        protected StringBuilder appendSuffix2(FsPath prefix, FsPath length, StringBuilder s)
        {
            if (!prefix.isRoot()) {
                throw new IllegalArgumentException(prefix + " is not a prefix of " + this);
            }
            return s.append('/');
        }

        @Override
        protected void write(ObjectOutput out, int len) throws IOException
        {
            out.writeShort(len);
        }
    }

    private static final class Child extends FsPath
    {
        private final FsPath parent;
        private final String name;

        private Child(FsPath parent, String name)
        {
            this.parent = parent;
            this.name = name;
        }

        @Override
        protected StringBuilder appendTo(StringBuilder path)
        {
            return parent.appendTo2(path).append(name);
        }

        @Override
        protected StringBuilder appendTo2(StringBuilder path)
        {
            return appendTo(path).append('/');
        }

        @Override
        public boolean isRoot()
        {
            return false;
        }

        @Override
        public FsPath parent()
        {
            return parent;
        }

        @Override
        public int hashCode()
        {
            return 31 * parent.hashCode() + name.hashCode();
        }

        @Override
        public boolean equals(Object o)
        {
            if (!(o instanceof FsPath)) {
                return false;
            }
            FsPath other = (FsPath) o;
            return !other.isRoot() && name().equals(other.name()) && parent().equals(other.parent());
        }

        @Override
        public String name()
        {
            return name;
        }

        @Override
        public int length()
        {
            return parent().length() + 1;
        }

        @Override
        public FsPath drop(int n)
        {
            checkArgument(n >= 0);
            return (n == 0) ? this : parent().drop(n - 1);
        }

        @Override
        protected FsPath tryDropLength(FsPath path)
        {
            return path.isRoot() ? this : parent().tryDropLength(path.parent());
        }

        @Override
        protected FsPath dropLength(FsPath path)
        {
            return path.isRoot() ? this : parent().dropLength(path.parent());
        }

        @Override
        protected boolean isSuffix(Iterator<String> path)
        {
            return !path.hasNext() || path.next().equals(name()) && parent().isSuffix(path);
        }

        @Override
        protected boolean contains(List<String> path)
        {
            return isSuffix(path.iterator()) || parent().contains(path);
        }

        @Override
        protected StringBuilder appendSuffix(FsPath prefix, FsPath length, StringBuilder s)
        {
            if (length.isRoot()) {
                if (!equals(prefix)) {
                    throw new IllegalArgumentException(prefix + " is not a prefix of " + this);
                }
                return s;
            }
            return parent().appendSuffix2(prefix, length.parent(), s).append(name());
        }

        @Override
        protected StringBuilder appendSuffix2(FsPath prefix, FsPath length, StringBuilder s)
        {
            return appendSuffix(prefix, length, s).append('/');
        }

        @Override
        protected void write(ObjectOutput out, int len) throws IOException
        {
            parent.write(out, len + 1);
            out.writeUTF(name);
        }
    }

    private static class SerializedPath implements Externalizable
    {
        private static final long serialVersionUID = 2301167077307955976L;

        private transient FsPath path;

        public SerializedPath()
        {
        }

        public SerializedPath(FsPath path)
        {
            this.path = path;
        }

        @Override
        public void writeExternal(ObjectOutput out) throws IOException
        {
            path.write(out, 0);
        }

        @Override
        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException
        {
            this.path = ROOT.read(in, in.readShort());
        }

        private Object readResolve()
        {
            return path;
        }
    }
}

