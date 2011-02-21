package org.dcache.gplazma.plugins;

import gplazma.authz.util.NameRolePair;

import java.util.Arrays;

class CachedMapsProvider {

    public static final String VALID_DN = "/O=GermanGrid/OU=DESY/CN=Tigran Mkrtchyan";
    public static final String VALID_FQAN_LONG_ROLE = "/dteam/Role=NULL/Capability=NULL";
    public static final String VALID_FQAN_SHORT_ROLE = "/dteam";
    public static final String VALID_USERNAME_RESPONSE = "tigran";
    public static final String VALID_ROLE_WC_USERNAME_RESPONSE = "dteamuser";
    public static final String VALID_WC_USERNAME_RESPONSE = "horst";
    public static final String INVALID_USERNAME = "SomeInvalidUser";

    public static final int VALID_USERNAME_UID = 3750;
    public static final int VALID_USERNAME_GID = 500;
    public static final int INVALID_GID = 666;
    public static final int INVALID_UID = 666;


    public static SourceBackedPredicateMap<NameRolePair, String> createCachedVOMapWithWildcards() {
        return new SourceBackedPredicateMap<NameRolePair, String>(new MemoryLineSource(Arrays.asList(
                "# some comment with a following empty line",
                "",
                "\""+VALID_DN+"\" \""+VALID_FQAN_LONG_ROLE+"\" "+VALID_USERNAME_RESPONSE,
                "\"*\" \""+VALID_FQAN_SHORT_ROLE+"*\" "+VALID_ROLE_WC_USERNAME_RESPONSE,
                "\""+VALID_DN+"\" \"/cms/Role=NULL/Capability=NULL\" tigrancms # comment",
                "* "+VALID_WC_USERNAME_RESPONSE,
                "\"/O=GermanGrid/OU=DESY/CN=Klaus Maus\" \""+VALID_FQAN_LONG_ROLE+"\" klaus # comment")
        ), new VOMapLineParser());
    }

    public static  SourceBackedPredicateMap<NameRolePair, String> createCachedVOMap() {
        return new SourceBackedPredicateMap<NameRolePair, String>(new MemoryLineSource(Arrays.asList(
                "# some comment with a following empty line",
                "",
                "\""+VALID_DN+"\" \""+VALID_FQAN_LONG_ROLE+"\" "+VALID_USERNAME_RESPONSE,
                "\""+VALID_DN+"\" \"/cms/Role=NULL/Capability=NULL\" tigrancms # comment",
                "\"/O=GermanGrid/OU=DESY/CN=Klaus Maus\" \""+VALID_FQAN_LONG_ROLE+"\" klaus # comment")
        ), new VOMapLineParser());
    }

    public static SourceBackedPredicateMap<String, AuthzMapLineParser.UserAuthzInformation> createCachedAuthzMap() {
        return new SourceBackedPredicateMap<String, AuthzMapLineParser.UserAuthzInformation>(new MemoryLineSource(Arrays.asList("# storage-authzdb",
                "# ----------------------------------------------------------------------------------------------------",
                "# Repository of Storage Element (SRM-dCache-PNFS) records for all authorized users",
                "# Required format of a record:",
                "# WS authorize WS username read-only|read-write WS UID WS GID WS homepath WS rootpath WS [fs-rootpath WS]",
                "# Example:",
                "# authorize timur    read-write    500 500       / /pnfs/fnal.gov /pnfs/fnal.gov",
                "# authorize rana     read-write    500 19000     / /pnfs/fnal.gov/data /pnfs/fnal.gov/data",
                "# ----------------------------------------------------------------------------------------------------",
                "",
                "version 2.1",
                "# Version scheme for initial version compatibility with dcache.kpwd directly used by SRM-dCache-PNFS",
                "",
                "# ----------------------------------------------------------------------------------------------------",
                "# authorize USER     PRIVILEGE     UID GID       HOME ROOT [FSROOT]",
                "# ----------------------------------------------------------------------------------------------------",
                "",
                "authorize "+VALID_USERNAME_RESPONSE+"        read-write    "+VALID_USERNAME_UID+" "+VALID_USERNAME_GID+"       \"/ fff/fff/!@# $% /\" / ",
                "authorize "+VALID_ROLE_WC_USERNAME_RESPONSE+"     read-write    1001 101       \"/ fdfd fdf/\" /",
                "authorize tigrandteam     read-write    "+VALID_USERNAME_UID+" 501       \"/ fff/fff/!@# $% /\" / ")), new AuthzMapLineParser());
    }
}
