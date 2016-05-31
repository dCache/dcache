# Invokes indexer
billing_indexer() # $* = arguments
{
    CLASSPATH="$(printLimitedClassPath slf4j-api logback-classic logback-core logback-console-config \
        jul-to-slf4j commons-compress gson spring-core guava dcache-common common-cli cells dcache-core \
        curator-client)" \
        quickJava \
          "-Ddcache.home=${DCACHE_HOME}" \
          "-Ddcache.paths.defaults=${DCACHE_DEFAULTS}" \
          org.dcache.services.billing.text.Indexer "$@"
}

# Computes index for billing file
billing_index_yesterday()
{
    billing_indexer 0.01 "$1"
}

# (Re)indexes all billing files
billing_index_all()
{
    billing_indexer -all 0.01
}

# Scan billing files for search term
billing_find()
{
    billing_indexer -find "$@"
}
