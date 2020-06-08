#!/usr/bin/bash

#find ./inc -name "*.xml*" > temp.txt
#echo ./osm.xml >> temp.txt
# templates rausnehmen und sowas

echo '<!ENTITY date "('"'23.05.2020'" 'between valid_since and valid_until) and">' >> ./inc/entities.xml.inc && python select_changer.py temp.txt



exit 0
