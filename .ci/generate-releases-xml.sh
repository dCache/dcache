#!/bin/sh


SRM_RPM_NAME=`ls /home/marina/dcache/modules/srm-client/target/rpmbuild/RPMS/noarch/ | grep dcache-srmclient`
SRM_RPM_SUM=`md5sum /home/marina/dcache/modules/srm-client/target/rpmbuild/RPMS/noarch/$SRM_RPM_NAME | cut -d ' ' -f 1`
DEB_NAME=`ls /home/marina/dcache/packages/fhs/target/ | grep dcache`
DEB_SUM=`md5sum /home/marina/dcache/packages/fhs/target/$DEB_NAME | cut -d ' ' -f 1`
TAR_NAME=`ls /home/marina/dcache/packages/tar/target/ | grep dcache`
TAR_SUM=`md5sum /home/marina/dcache/packages/tar/target/$TAR_NAME | cut -d ' ' -f 1`
RPM_NAME=`ls /home/marina/dcache/packages/fhs/target/rpmbuild/RPMS/noarch/ | grep dcache`
RPM_SUM=`md5sum /home/marina/dcache/packages/fhs/target/rpmbuild/RPMS/noarch/$RPM_NAME | cut -d ' ' -f 1`
DATE=`date +"%Y.%m.%d"`

echo "| Download   | Build date | md5 sum  |"
echo "|:-----------|:-----------|----------|"
echo "| $RPM_NAME  | $DATE      | $RPM_SUM |"
echo "| $DEB_NAME  | $DATE      | $DEB_SUM |"
echo "| $TAR_NAME  | $DATE      | $TAR_SUM |"
echo "| $SRM_RPM_NAME | $DATE   | $SRM_RPM_SUM |"

echo ; echo; echo
git log `git describe --tags --abbrev=0`...`git describe --tags --abbrev=0 HEAD^` --no-merges --format='[%h](https://github.com/dcache/dcache/commit/%H)%n:    %s%n'



my_string="10.0.13"
before_last_dot=$(echo "$my_string" | rev | cut -d'.' -f2- | rev)
echo "$before_last_dot"



series=$(echo "$my_string" | rev | cut -d'.' -f2- | rev)
echo "TES series $series"


get_series() { # $1 dCache version
    echo ${my_string%%.*([0-9])}
}

get_bugfix() { # $1 dCache version
    echo ${my_string##*([0-9]).*([0-9]).}
}

echo "TEST 1 ${my_string##*([0-9]).*([0-9]).}"

echo "Test 2 ${my_string%%.*([0-9])}"

file="releases-$(echo $series).html"
echo "test html file name $file"

update_releases() { # $1 - dCache version
	# old web pages
	#[ -z ${date+x} ] || date_param="--stringparam date $date"
  #  xsltproc --stringparam version $1 $date_param --stringparam checksums-path "$(pwd)" $share/update-releases.xsl releases.xml > out.xml
  #  mv out.xml releases.xml

	# new web pages
    [ -z ${date} ] && date=$(date +%d.%m.%Y)
	series=$(get_series $my_string)
	bugfix=$(get_bugfix $my_string)
	file="releases-$(echo $series).html"

	echo "test file name $file"


	# replace rec (= recent version, green highlighted) with even or odd
	# if newest version is odd, the last version was even and vice versa
	even_odd="odd"
	mod=$((13 % 2))
	[ $mod -eq "0" ] || even_odd="even"

	tmp_file="$file.tmp"
	touch $tmp_file
	sed "s/rec/${even_odd}/" $file > $tmp_file

	mv $tmp_file $file

	echo "TEST $tmp_file"

		echo $file

echo

	# write new table entry on top
	cat > releases.xml << EOF

	<div class="test">
      <style>
        table.releases tbody tr.rec { background-color: #9f9; }
      	table.releases tbody tr.odd { background-color: #e8f6f8; }
      	table.releases tbody tr.even { background-color: #d0e5e8; }
      </style>

 <table border="1">
   <thead>
 	<tr>
 	    <th>Download</th>
 	    <th>Rel. Date</th>
 	    <th>md5 hash</th>
 	    <th>Release Notes</th>
     	</tr>
 	</thead>
   <!-- First Row -->
   <tr class="rec" id="$my_string">
     <td class="link">
       <a href="/old/downloads/1.9/repo/$series/$DEB_NAME">
         dCache $my_string (Debian package)
       </a>
     </td>
     <td class="date">$DATE</td>
     <td class="hash">$DEB_SUM</td>
     <td class="notes" rowspan="3">
       <a href="/old/downloads/1.9/release-notes-$series.shtml#release$my_string">
         $my_string
       </a>
     </td>
   </tr>

   <!-- Second Row -->
   <tr class="rec">
     <td class="link">
       <a href="/old/downloads/1.9/repo/$series/$RPM_NAME">
         dCache $my_string (rpm)
       </a>
     </td>
     <td class="date">$DATE</td>
     <td class="hash">$RPM_SUM</td>
   </tr>

   <!-- Third Row -->
   <tr class="rec">
     <td class="link">
       <a href="/old/downloads/1.9/repo/$series/$TAR_NAME">
         dCache $my_string (tgz)
       </a>
     </td>
     <td class="date">$DATE</td>
     <td class="hash">$TAR_SUM</td>
   </tr>
 </table>
</div>
EOF

# Add old table entries below the new one
cat $file >> $tmp_file
mv $tmp_file $file
}

update_releases

