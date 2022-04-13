# Useful functions for working with dCache databases.

hasDatabase() # $1 = domain, $2 = cell
{
    [ -n "$(getScopedProperty db.name "$1" "$2")" ]
}

hasManagedDatabase() # $1 = domain, $2 = cell
{
    [ -n "$(getScopedProperty db.schema.changelog "$1" "$2")" ]
}

hasAutoSchema() # $1 = domain, $2 = cell
{
    [ "$(getScopedProperty db.schema.auto "$1" "$2")" = "true" ]
}

liquibase() # $1 = domain, $2 = cell, $3+ = liquibase arguments
{
    local url
    local user
    local password
    local driver
    local classpath
    local liquibase
    local changelog

    url=$(getScopedProperty db.url "$1" "$2")
    user=$(getScopedProperty db.user "$1" "$2")
    password=$(getScopedProperty db.password "$1" "$2")
    driver=$(getScopedProperty db.driver "$1" "$2")
    classpath=$(printLimitedClassPath liquibase-core commons-lang3 commons-io opencsv snakeyaml picocli jaxb-api jaxb-core jaxb-impl slf4j-api logback-classic \
                logback-core dcache-core dcache-bulk dcache-qos dcache-spacemanager srm-server chimera postgresql hsqldb h2)
    changelog=$(getScopedProperty db.schema.changelog "$1" "$2")
    changelogpre=$(getScopedProperty db.schema.changelog-pre "$1" "$2")

    shift 2

    # Apply changelog that fixes liquibase incompatibility issues
    if [ "x${changelogpre}" != "x" ]
    then
        CLASSPATH="$classpath" quickJava liquibase.integration.commandline.LiquibaseCommandLine --headless=true --log-channels=all --log-level=WARNING --show-banner=false --driver="${driver}" --changeLogFile="${changelogpre}" --url="${url}" --username="${user}" --password="${password}" "$@"
    fi
    CLASSPATH="$classpath" quickJava liquibase.integration.commandline.LiquibaseCommandLine --headless=true --log-channels=all --log-level=WARNING --show-banner=false --driver="${driver}" --changeLogFile="${changelog}" --url="${url}" --username="${user}" --password="${password}" "$@"
}
