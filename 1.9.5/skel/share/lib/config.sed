# Sed script to put configuration files into a normal form. Handles
# comments, line continuation, white space around equal sign, quoted
# values and spaces in values. Escapes the backslash, quote, backtick
# and dollar symbols. Dollar symbols are not escaped if they are used
# for variable substitution (i.e. they are followed by a alphabetic
# character or an opening curlybrace). 
#
# Empty lines and non-assignment lines are stripped.
#
# Does not handle multi-line quoted strings or multible quoted values.
#

# Delete pure comment lines
/^#/ d

# Delete empty lines 
/^$/ d

:repeat

# Strip trailing comments
s/\([^\#]*[ 	]\)\#.*/\1/

# Repeat on line continuation
/\\$/ {
    N
    s/\\\n//
    b repeat
}

# Strip leading white space
s/^[ 	]*//
# Strip trailing white space
s/[ 	]*$//
# Strip space around assignment
s/\([^=]*\)[ 	]*=[ 	]*\(.*\)/\1=\2/

/[^=]*='.*'/ {
    # Unquote value
    s/\([^=]*\)='\(.*\)'/\1=\2/
    # Skip second quoting style
    b quoted
}
# Unquote value
s/\([^=]*\)="\(.*\)"/\1=\2/
:quoted

# Escape backslash
s/\\/\\\\/g
# Escape quotes
s/'/\\'/g
# Escape backtick
s/`/\\`/g
# Escape most $
s/\$\([^a-zA-Z_{]\)/\\$\1/g
# Escape trailing $
s/\$$/\\\$/g
# Quote value
s/\([^=]*\)=\(.*\)/\1="\2"/
# Delete if not an assignment
/[a-zA-Z0-9_]*=".*"/ !d
