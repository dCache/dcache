# Useful functions for working with dCache databases.

hasDatabase() # $1 = domain, $2 = cell
{
    [ -n "$(getProperty db.name "$1" "$2")" ]
}

hasManagedDatabase() # $1 = domain, $2 = cell
{
    [ -n "$(getProperty db.schema.changelog "$1" "$2")" ]
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

    url=$(getProperty db.url "$1" "$2")
    user=$(getProperty db.user "$1" "$2")
    password=$(getProperty db.password "$1" "$2")
    driver=$(getProperty db.driver "$1" "$2")
    classpath=$(printClassPath "$1")
    changelog=$(getProperty db.schema.changelog "$1" "$2")

    shift 2
    CLASSPATH="$classpath" quickJava liquibase.integration.commandline.Main --driver="${driver}" --changeLogFile="${changelog}" --url="${url}" --username="${user}" --password="${password}" "$@"
}
