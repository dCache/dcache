#this file must be sourced not executed!

#Find the ups setup script

possibleLocations="/usr/local/etc/setups.sh  /fnal/ups/etc/setups.sh /local/ups/etc/setups.sh"

for i in $possibleLocations; do
    if [ -r $i ]; then
        . $i
	break
    fi
done
