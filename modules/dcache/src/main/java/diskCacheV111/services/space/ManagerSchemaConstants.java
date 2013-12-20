package diskCacheV111.services.space;

/**
 * Database storage related variables.
 */
public class ManagerSchemaConstants {
	public static final String POPULATE_USED_SPACE_IN_SRMSPACE_TABLE_BY_ID =
		" update srmspace set usedspaceinbytes=( "+
		" select coalesce(sum(sf.sizeinbytes),0) "+
		" from srmspace s left outer join srmspacefile sf on "+
		" s.id=sf.spacereservationid and sf.state=2 and s.id=?),"+
                " allocatedspaceinbytes= ("+
		" select coalesce(sum(sf.sizeinbytes),0) "+
		" from srmspace s left outer join srmspacefile sf on "+
		" s.id=sf.spacereservationid and sf.state<2 and s.id=?) where srmspace.id=?";

	public static final String POPULATE_RESERVED_SPACE_IN_SRMLINKGROUP_TABLE_BY_ID =
		" update srmlinkgroup set reservedspaceinbytes=( "+
		" select coalesce(sum(s.sizeinbytes-s.usedspaceinbytes),0) "+
		" from srmlinkgroup lg left outer join srmspace s on "+
		" s.linkGroupId=lg.id and lg.id=? and s.state=0) where  srmlinkgroup.id=?";

	public static final String SPACE_MANAGER_NEXT_ID_TABLE_NAME =
		"srmspacemanagernextid";
	public static final String LINK_GROUP_VOS_TABLE_NAME =
		"srmLinkGroupVOs".toLowerCase();
	public static final String RETENTION_POLICY_TABLE_NAME =
		"srmRetentionPolicy".toLowerCase();
	public static final String ACCESS_LATENCY_TABLE_NAME =
		"srmAccessLatency".toLowerCase();
	public static final String SPACE_TABLE_NAME =
		"srmSpace".toLowerCase();
	public static final String SPACE_FILE_TABLE_NAME =
		"srmSpaceFile".toLowerCase();
}
