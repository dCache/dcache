package org.dcache.auth;

import com.google.common.base.Joiner;

import java.util.Set;
import java.util.TreeSet;

public class UserAuthRecord extends UserAuthBase {
    private static final long serialVersionUID = 2212212275053022221L;

    public Set<String> principals;

    public UserAuthRecord(String user,
                          String DN,
                          String fqan,
			              boolean readOnly,
                          int priority,
                          int uid,
                          int[] GIDs,
                          String home,
                          String root,
                          String fsroot,
                          Set<String> principals) {
        super(user, DN, fqan, readOnly, priority, uid, GIDs, home, root, fsroot);
        this.principals = new TreeSet<>(principals);
    }

    public UserAuthRecord(String user,
                          String DN,
                          String fqan,
			              boolean readOnly,
                          int priority,
                          int uid,
                          int gid,
                          String home,
                          String root,
                          String fsroot,
                          Set<String> principals) {
        super(user, DN, fqan, readOnly, priority, uid, gid, home, root, fsroot);
        this.principals = new TreeSet<>(principals);
    }


    public UserAuthRecord(String user,
			              boolean readOnly,
                          int uid,
                          int[] GIDs,
                          String home,
                          String root,
                          String fsroot,
                          Set<String> principals) {
        super(user, readOnly, uid, GIDs, home, root, fsroot);
        this.principals = new TreeSet<>(principals);
    }

    public UserAuthRecord(String user,
			              boolean readOnly,
                          int uid,
                          int gid,
                          String home,
                          String root,
                          String fsroot,
                          Set<String> principals) {
        super(user, readOnly, uid, gid, home, root, fsroot);
        this.principals = new TreeSet<>(principals);
    }

    /**
     * nonprivate default constructor to satisfy the JPA requirements
     */
    public UserAuthRecord() {
    }


    public void appendToStringBuffer(StringBuffer sb) {
        sb.append(Username);
        if(ReadOnly) {
            sb.append(" read-only");
        } else {
            sb.append(" read-write");
        }
        sb.append( ' ').append( UID).append( ' ');
        sb.append(Joiner.on(",").skipNulls().join(GIDs.iterator())).append(' ');
        sb.append( Home ).append(' ');
        sb.append( Root ).append(' ');
        sb.append( FsRoot ).append('\n');
        if(principals != null) {
            for(String principal : principals) {
                sb.append("  ").append(principal).append('\n');
            }
        }
    }


    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(Username);
        sb.append(' ').append( DN);
        sb.append(' ').append( getFqan());
          if(ReadOnly) {
            sb.append(" read-only");
          } else {
            sb.append(" read-write");
          }
        sb.append( ' ').append( UID).append( ' ');
        sb.append( GIDs ).append(' ');
        sb.append( Home ).append(' ');
        sb.append( Root ).append(' ');
        sb.append( FsRoot ).append('\n');
        if(principals != null) {
            for(String principal : principals) {
                sb.append("  ").append(principal).append('\n');
            }
        }
        return sb.toString();
    }

    public String toDetailedString() {
        StringBuilder sb = new StringBuilder(" User Authentication Record for ");
        sb.append(Username).append(" :\n");
        sb.append("             DN = ").append(DN).append('\n');
        sb.append("           FQAN = ").append(getFqan()).append('\n');
	      sb.append("      read-only = ").append(readOnlyStr())
                      .append('\n');
        sb.append("            UID = ").append(UID).append('\n');
        sb.append("           GIDs = ");
        sb.append(Joiner.on(",").skipNulls().join(GIDs.iterator())).append('\n');
        sb.append("           Home = ").append(Home).append('\n');
        sb.append("           Root = ").append(Root).append('\n');
        sb.append("         FsRoot = ").append(FsRoot).append('\n');

        if(principals != null) {
            sb.append("         Secure Ids accepted by this user :\n");
            for(String principal : principals) {
                sb.append("    SecureId  = \"").append(principal).append("\"\n");
            }
        }
        return sb.toString();
    }

    @Override
    public boolean isAnonymous() {
        return false;
    }

    @Override
    public boolean isWeak() {
        return false;
    }

    public boolean hasSecureIdentity(String p) {
      if(principals!=null) {
          return principals.contains(p);
      }
      return false;
    }

    public boolean isValid() {
        return Username != null;
    }

    public void addSecureIdentity(String id) {
        principals.add(id);
    }

    public void addSecureIdentities(Set<String> ids) {
        principals.addAll(ids);
    }

    public void removeSecureIdentities(Set<String> ids) {
        principals.removeAll(ids);
    }

  @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj == null || !(obj instanceof UserAuthRecord)) {
            return false;
        }
        UserAuthRecord r = (UserAuthRecord) obj;
        return Username.equals(r.Username) && ReadOnly == r.ReadOnly
                && UID == r.UID && GIDs.equals(r.GIDs)
                && Home.equals(r.Home) && Root.equals(r.Root)
                && FsRoot.equals(r.FsRoot) && this.getFqan().equals(r.getFqan());
    }
}

