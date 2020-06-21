# Preselected Rendering

This project is a proof of concept. It aims to use mapnik 2 to render data under certain conditions mapnik itself has no concept of. In our case this condition is time (OHDM-Project: LINK).
To ensure simple usage, the goal was to change as little as possible about mapnik and the original stylesheets.

- To read about the concept and example-implementation, please refer to LINK.
- The installation guide is found [here](https://github.com/OpenHistoricalDataMap/Preselected-Rendering/wiki/Deployment).

## Files and Directories

### generate_image.py
A script (from the original repository [OpenStreetMap/mapnik-stylesheets](https://github.com/openstreetmap/mapnik-stylesheets)) to generate a map image from OSM data using Mapnik. It will read mapping instructions from 'osm.xml' and write the finished map to 'image.png'. You have to change the script to change the bounding box or image size.

This script does not use preselection and is kept in this repository solely for testing purposes.

---
	
### generate_view_tiles.py
The heart of this project. This script is a heavily modified version of generate_tiles.py from the original repository [OpenStreetMap/mapnik-stylesheets](https://github.com/openstreetmap/mapnik-stylesheets). The original script is completely included, with just the executional part commented out.

generate_view_tiles.py renders preselected OSM data using Mapnik. It creates Views in the database given in 'datasource-settings.xml.inc', changes the prefix in 'settings.xml.inc' to match them and then reads mapping instructions from 'osm.xml' and writes the finished maps to the directory defined in the constant TILE_DIR as follows:
	
	[TILE_DIR] / [DATE] / [ZOOM] / [X] / [Y].png

You can change the constants at the top of this script to match your directory configuration.
Usage is documented at LINK.

---

### osm.xml
The file which contains the rules on how Mapnik should render data. Every occurence of the table '_roads' is replaced by '_line', otherwise this file is the same as in the [OpenStreetMap/mapnik-stylesheets](https://github.com/openstreetmap/mapnik-stylesheets) repository.

---

### inc/
This directory contains additional rule-sets for the rendering process which are referenced by 'osm.xml'. Every occurence of the table '_roads' is replaced by '_line', otherwise these files are the same as in the [OpenStreetMap/mapnik-stylesheets](https://github.com/openstreetmap/mapnik-stylesheets) repository. The original directory contained .template-files as well, but they are not needed for this proof of concept.

---

### symbols/
Directory with icons and highway shield images.

---

### world_boundaries/ (will not be included)
Directory containing shapefiles to render current borders and coastlines. Since this is fairly useless in the context of time-sensitive rendering, please refer to our documentation if you want to change it: LINK.
