package diskCacheV111.services.space;

public class ManagerSchemaConstants {
	/*
	 * Database storage related variables
	 */
	public static final int currentSchemaVersion = 2;
	public static final String SpaceManagerSchemaVersionTableName =
		"srmspacemanagerschemaversion";

	public static final String selectSpaceManagerSchemaVersion=
		"select version from "+SpaceManagerSchemaVersionTableName;

	public static final String POPULATE_USED_SPACE_IN_SRMSPACE_TABLE =
		" update srmspace set usedspaceinbytes=( "+
		" select coalesce(sum(sf.sizeinbytes),0) "+
		" from srmspace s left outer join srmspacefile sf on "+
		" s.id=sf.spacereservationid and sf.state=2 and s.id=srmspace.id),"+
                " allocatedspaceinbytes= ("+
		" select coalesce(sum(sf.sizeinbytes),0) "+
		" from srmspace s left outer join srmspacefile sf on "+
		" s.id=sf.spacereservationid and sf.state<2 and s.id=srmspace.id)";

	public static final String POPULATE_USED_SPACE_IN_SRMSPACE_TABLE_BY_ID =
		" update srmspace set usedspaceinbytes=( "+
		" select coalesce(sum(sf.sizeinbytes),0) "+
		" from srmspace s left outer join srmspacefile sf on "+
		" s.id=sf.spacereservationid and sf.state=2 and s.id=?),"+
                " allocatedspaceinbytes= ("+
		" select coalesce(sum(sf.sizeinbytes),0) "+
		" from srmspace s left outer join srmspacefile sf on "+
		" s.id=sf.spacereservationid and sf.state<2 and s.id=?) where srmspace.id=?";

	public static final String POPULATE_RESERVED_SPACE_IN_SRMLINKGROUP_TABLE =
		" update srmlinkgroup set reservedspaceinbytes=( "+
		" select coalesce(sum(s.sizeinbytes-s.usedspaceinbytes),0) "+
		" from srmlinkgroup lg left outer join srmspace s on "+
		" s.linkGroupId=lg.id and lg.id=srmlinkgroup.id and s.state=0) ";

	public static final String POPULATE_RESERVED_SPACE_IN_SRMLINKGROUP_TABLE_BY_ID =
		" update srmlinkgroup set reservedspaceinbytes=( "+
		" select coalesce(sum(s.sizeinbytes-s.usedspaceinbytes),0) "+
		" from srmlinkgroup lg left outer join srmspace s on "+
		" s.linkGroupId=lg.id and lg.id=? and s.state=0) where  srmlinkgroup.id=?";

	public static final String SpaceManagerNextIdTableName =
		"srmspacemanagernextid";
	public static final String LinkGroupTableName =
		"srmLinkGroup".toLowerCase();
	public static final String LinkGroupVOsTableName =
		"srmLinkGroupVOs".toLowerCase();
	public static final String RetentionPolicyTableName =
		"srmRetentionPolicy".toLowerCase();
	public static final String AccessLatencyTableName =
		"srmAccessLatency".toLowerCase();
	public static final String SpaceTableName =
		"srmSpace".toLowerCase();
	public static final String SpaceFileTableName =
		"srmSpaceFile".toLowerCase();
	protected static final String stringType=" VARCHAR(32672) ";
	protected static final String longType=" BIGINT ";
	protected static final String intType=" INTEGER ";
	protected static final String dateTimeType= " TIMESTAMP ";
	protected static final String booleanType= " INT ";
	public static final String CreateSpaceManagerSchemaVersionTable =
		"CREATE TABLE "+SpaceManagerSchemaVersionTableName+
		" ( version "+ intType + " )";
	public static final String CreateSpaceManagerNextIdTable =
		"CREATE TABLE "+SpaceManagerNextIdTableName+
		" ( NextToken "+ longType + " )";

	public static final String CreateLinkGroupTable =
		"CREATE TABLE "+ LinkGroupTableName+" ( "+
		" id "+longType+" NOT NULL PRIMARY KEY "+
		", name"+stringType+" " +
		", freeSpaceInBytes "+longType+" "+
		", lastUpdateTime "+longType +
		", onlineAllowed"+booleanType+" "+
		", nearlineAllowed"+booleanType+" "+
		", replicaAllowed"+booleanType+" "+
		", outputAllowed"+booleanType+" "+
		", custodialAllowed"+booleanType+" "+
                ", reservedspaceinbytes "+longType+" "+
            ")";

	public static final String CreateLinkGroupVOsTable =
		"CREATE TABLE "+ LinkGroupVOsTableName+" ( "+
		" VOGroup "+stringType+" NOT NULL "+
		", VORole "+stringType+" NOT NULL "+
		", linkGroupId "+longType+" NOT NULL "+
		", PRIMARY KEY (VOGroup, VORole, linkGroupId) "+
		", CONSTRAINT fk_"+LinkGroupVOsTableName+
		"_L FOREIGN KEY (linkGroupId) REFERENCES "+
		LinkGroupTableName +" (id) "+
		" ON DELETE RESTRICT"+
		")";

	public static final String CreateRetentionPolicyTable =
		"CREATE TABLE "+ RetentionPolicyTableName+" ( "+
		" id "+intType+" NOT NULL PRIMARY KEY "+
		", name "+stringType+" )";

	public static final String CreateAccessLatencyTable =
		"CREATE TABLE "+ AccessLatencyTableName+" ( "+
		" id "+intType+" NOT NULL PRIMARY KEY "+
		", name "+stringType+" )";

	public static final String CreateSpaceTable =
		"CREATE TABLE "+ SpaceTableName+" ( "+
		" id "+longType+" NOT NULL PRIMARY KEY "+
		", voGroup "+stringType+" "+
		", voRole "+stringType+" "+
		", retentionPolicy "+intType+" "+
		", accessLatency "+intType+" "+
		", linkGroupId "+longType+" "+
		", sizeInBytes "+longType+" "+
		", creationTime "+longType+" "+
		", lifetime "+longType+" "+
		", description "+stringType+" "+
		", state "+intType+" "+
                ", usedspaceinbytes "+longType+" "+
		", allocatedspaceinbytes "+longType+" "+
		", CONSTRAINT fk_"+SpaceTableName+
		"_L FOREIGN KEY (linkGroupId) REFERENCES "+
		LinkGroupTableName +" (id) "+
		", CONSTRAINT fk_"+SpaceTableName+
		"_A FOREIGN KEY (accessLatency) REFERENCES "+
		AccessLatencyTableName +" (id) "+
		", CONSTRAINT fk_"+SpaceTableName+
		"_R FOREIGN KEY (retentionPolicy) REFERENCES "+
		RetentionPolicyTableName +" (id) "+
		" ON DELETE RESTRICT"+
            ")";

    public static final String CreateSpaceFileTable =
            "CREATE TABLE "+ SpaceFileTableName+" ( "+
            " id "+longType+" NOT NULL PRIMARY KEY "+
            ", voGroup "+stringType+" "+
            ", voRole "+stringType+" "+
            ", spaceReservationId "+longType+" "+
            ", sizeInBytes "+longType+" "+
            ", creationTime "+longType+" "+
            ", lifetime "+longType+" "+
            ", pnfsPath "+stringType+" "+
            ", pnfsId "+stringType+" "+
            ", state "+intType+" "+
            ", deleted "+intType+" "+
            ", CONSTRAINT fk_"+SpaceFileTableName+
            "_L FOREIGN KEY (spaceReservationId) REFERENCES "+
            SpaceTableName +" (id) "+
            " ON DELETE RESTRICT"+
            ")";
}
