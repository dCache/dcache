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

/^#/ d                                      # Delete pure comment lines
/^$/ d                                      # Delete empty lines 

:repeat

s/\([^\#]*[ 	]\)\#.*/\1/                 # Strip trailing comments

/\\$/ {                                     # Repeat on line continuation
    N
    s/\\\n//
    b repeat
}

s/^[ 	]*//                                # Strip leading white space
s/[ 	]*$//                               # Strip trailing white space

s/\([^=]*\)[ 	]*=[ 	]*\(.*\)/\1=\2/     # Strip space around assignment

/[^=]*='.*'/ {
    s/\([^=]*\)='\(.*\)'/\1=\2/             # Unquote value
    b was_quoted                            # Skip second quoting style
}
s/\([^=]*\)="\(.*\)"/\1=\2/                 # Unquote value
:was_quoted

s/\\/\\\\/g                                 # Escape backslash
s/'/\\'/g                                   # Escape quotes
s/`/\\`/g                                   # Escape backtick
s/\$\([^a-zA-Z_{]\)/\\$\1/g                 # Escape most $
s/\$$/\\\$/g                                # Escape trailing $

s/\([^=]*\)=\(.*\)/\1="\2"/                 # Quote value

/[a-zA-Z0-9_]*=".*"/ !d                     # Delete if not an assignment
