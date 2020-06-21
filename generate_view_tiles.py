#!/usr/bin/env python
from math import pi, cos, sin, log, exp, atan
from subprocess import call
import sys, os
from Queue import Queue

import threading

import mapnik

import socket
import psycopg2
import datetime
import getopt

#################################################################################################
## PSR constants
#################################################################################################

DIRNAME = os.path.dirname(__file__)
DATABASE_SOURCE = os.path.join(DIRNAME, 'inc/datasource-settings.xml.inc')
PREFIX_SOURCE = os.path.join(DIRNAME, 'inc/settings.xml.inc')
TILE_DIR = os.environ['HOME'] + "/osm/tiles/"

#################################################################################################
## ORIGINAL GENERATE_TILES.PY
#################################################################################################

# Default number of rendering threads to spawn, should be roughly equal to number of CPU cores available
NUM_THREADS = 4

DEG_TO_RAD = pi / 180
RAD_TO_DEG = 180 / pi


def minmax(a, b, c):
    a = max(a, b)
    a = min(a, c)
    return a


class GoogleProjection:
    def __init__(self, levels=18):
        self.Bc = []
        self.Cc = []
        self.zc = []
        self.Ac = []
        c = 256
        for d in range(0, levels):
            e = c / 2
            self.Bc.append(c / 360.0)
            self.Cc.append(c / (2 * pi))
            self.zc.append((e, e))
            self.Ac.append(c)
            c *= 2

    def fromLLtoPixel(self, ll, zoom):
        d = self.zc[zoom]
        e = round(d[0] + ll[0] * self.Bc[zoom])
        f = minmax(sin(DEG_TO_RAD * ll[1]), -0.9999, 0.9999)
        g = round(d[1] + 0.5 * log((1 + f) / (1 - f)) * -self.Cc[zoom])
        return (e, g)

    def fromPixelToLL(self, px, zoom):
        e = self.zc[zoom]
        f = (px[0] - e[0]) / self.Bc[zoom]
        g = (px[1] - e[1]) / -self.Cc[zoom]
        h = RAD_TO_DEG * (2 * atan(exp(g)) - 0.5 * pi)
        return (f, h)


class RenderThread:
    def __init__(self, tile_dir, mapfile, q, printLock, maxZoom):
        self.tile_dir = tile_dir
        self.q = q
        self.m = mapnik.Map(256, 256)
        self.printLock = printLock
        # Load style XML
        mapnik.load_map(self.m, mapfile, True)
        # Obtain <Map> projection
        self.prj = mapnik.Projection(self.m.srs)
        # Projects between tile pixel co-ordinates and LatLong (EPSG:4326)
        self.tileproj = GoogleProjection(maxZoom + 1)

    def render_tile(self, tile_uri, x, y, z):

        # Calculate pixel positions of bottom-left & top-right
        p0 = (x * 256, (y + 1) * 256)
        p1 = ((x + 1) * 256, y * 256)

        # Convert to LatLong (EPSG:4326)
        l0 = self.tileproj.fromPixelToLL(p0, z);
        l1 = self.tileproj.fromPixelToLL(p1, z);

        # Convert to map projection (e.g. mercator co-ords EPSG:900913)
        c0 = self.prj.forward(mapnik.Coord(l0[0], l0[1]))
        c1 = self.prj.forward(mapnik.Coord(l1[0], l1[1]))

        # Bounding box for the tile
        if hasattr(mapnik, 'mapnik_version') and mapnik.mapnik_version() >= 800:
            bbox = mapnik.Box2d(c0.x, c0.y, c1.x, c1.y)
        else:
            bbox = mapnik.Envelope(c0.x, c0.y, c1.x, c1.y)
        render_size = 256
        self.m.resize(render_size, render_size)
        self.m.zoom_to_box(bbox)
        if (self.m.buffer_size < 128):
            self.m.buffer_size = 128

        # Render image with default Agg renderer
        im = mapnik.Image(render_size, render_size)
        mapnik.render(self.m, im)
        im.save(tile_uri, 'png256')

    def loop(self):
        while True:
            # Fetch a tile from the queue and render it
            r = self.q.get()
            if (r == None):
                self.q.task_done()
                break
            else:
                (name, tile_uri, x, y, z) = r

            exists = ""
            if os.path.isfile(tile_uri):
                exists = "exists"
            else:
                self.render_tile(tile_uri, x, y, z)
            bytes = os.stat(tile_uri)[6]
            empty = ''
            if bytes == 103:
                empty = " Empty Tile "
            self.printLock.acquire()
            print name, ":", z, x, y, exists, empty
            self.printLock.release()
            self.q.task_done()


def render_tiles(bbox, mapfile, tile_dir, minZoom=1, maxZoom=18, name="unknown", num_threads=NUM_THREADS,
                 tms_scheme=False):
    print "render_tiles(", bbox, mapfile, tile_dir, minZoom, maxZoom, name, ")"

    # Launch rendering threads
    queue = Queue(32)
    printLock = threading.Lock()
    renderers = {}
    for i in range(num_threads):
        renderer = RenderThread(tile_dir, mapfile, queue, printLock, maxZoom)
        render_thread = threading.Thread(target=renderer.loop)
        render_thread.start()
        # print "Started render thread %s" % render_thread.getName()
        renderers[i] = render_thread

    if not os.path.isdir(tile_dir):
        os.mkdir(tile_dir)

    gprj = GoogleProjection(maxZoom + 1)

    ll0 = (bbox[0], bbox[3])
    ll1 = (bbox[2], bbox[1])

    for z in range(minZoom, maxZoom + 1):
        px0 = gprj.fromLLtoPixel(ll0, z)
        px1 = gprj.fromLLtoPixel(ll1, z)

        # check if we have directories in place
        zoom = "%s" % z
        if not os.path.isdir(tile_dir + zoom):
            os.mkdir(tile_dir + zoom)
        for x in range(int(px0[0] / 256.0), int(px1[0] / 256.0) + 1):
            # Validate x co-ordinate
            if (x < 0) or (x >= 2 ** z):
                continue
            # check if we have directories in place
            str_x = "%s" % x
            if not os.path.isdir(tile_dir + zoom + '/' + str_x):
                os.mkdir(tile_dir + zoom + '/' + str_x)
            for y in range(int(px0[1] / 256.0), int(px1[1] / 256.0) + 1):
                # Validate x co-ordinate
                if (y < 0) or (y >= 2 ** z):
                    continue
                # flip y to match OSGEO TMS spec
                if tms_scheme:
                    str_y = "%s" % ((2 ** z - 1) - y)
                else:
                    str_y = "%s" % y
                tile_uri = tile_dir + zoom + '/' + str_x + '/' + str_y + '.png'
                # Submit tile to be rendered into the queue
                t = (name, tile_uri, x, y, z)
                try:
                    queue.put(t)
                except KeyboardInterrupt:
                    raise SystemExit("Ctrl-c detected, exiting...")

    # Signal render threads to exit by sending empty request to queue
    for i in range(num_threads):
        queue.put(None)
    # wait for pending rendering jobs to complete
    queue.join()
    for i in range(num_threads):
        renderers[i].join()

""" this is the part of the original generate_tiles.py which was replaced
if __name__ == "__main__":
    home = os.environ['HOME']
    try:
        mapfile = os.environ['MAPNIK_MAP_FILE']
    except KeyError:
        mapfile = home + "/svn.openstreetmap.org/applications/rendering/mapnik/osm-local.xml"
    try:
        tile_dir = os.environ['MAPNIK_TILE_DIR']
    except KeyError:
        tile_dir = home + "/osm/tiles/"

    if not tile_dir.endswith('/'):
        tile_dir = tile_dir + '/'

    #-------------------------------------------------------------------------
    #
    # Change the following for different bounding boxes and zoom levels
    #
    # Start with an overview
    # World
    bbox = (-180.0,-90.0, 180.0,90.0)

    render_tiles(bbox, mapfile, tile_dir, 0, 5, "World")

    minZoom = 10
    maxZoom = 16
    bbox = (-2, 50.0,1.0,52.0)
    render_tiles(bbox, mapfile, tile_dir, minZoom, maxZoom)

    # Muenchen
    bbox = (11.4,48.07, 11.7,48.22)
    render_tiles(bbox, mapfile, tile_dir, 1, 12 , "Muenchen")

    # Muenchen+
    bbox = (11.3,48.01, 12.15,48.44)
    render_tiles(bbox, mapfile, tile_dir, 7, 12 , "Muenchen+")

    # Muenchen++
    bbox = (10.92,47.7, 12.24,48.61)
    render_tiles(bbox, mapfile, tile_dir, 7, 12 , "Muenchen++")

    # Nuernberg
    bbox=(10.903198,49.560441,49.633534,11.038085)
    render_tiles(bbox, mapfile, tile_dir, 10, 16, "Nuernberg")

    # Karlsruhe
    bbox=(8.179113,48.933617,8.489252,49.081707)
    render_tiles(bbox, mapfile, tile_dir, 10, 16, "Karlsruhe")

    # Karlsruhe+
    bbox = (8.3,48.95,8.5,49.05)
    render_tiles(bbox, mapfile, tile_dir, 1, 16, "Karlsruhe+")

    # Augsburg
    bbox = (8.3,48.95,8.5,49.05)
    render_tiles(bbox, mapfile, tile_dir, 1, 16, "Augsburg")

    # Augsburg+
    bbox=(10.773251,48.369594,10.883834,48.438577)
    render_tiles(bbox, mapfile, tile_dir, 10, 14, "Augsburg+")

    # Europe+
    bbox = (1.0,10.0, 20.6,50.0)
    render_tiles(bbox, mapfile, tile_dir, 1, 11 , "Europe+")
"""

#################################################################################################
## PRESELECTED RENDERING
#################################################################################################


#################################################################################################
## connection related
#################################################################################################


def doConnection(port):
    print "not yet"

    if port < 5000:
        print "Port number needs to be higher than 5000 but was " + str(port)
        sys.exit(1)

    address = ("localhost", int(port))

    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.bind(address)
        s.listen(1)
        print "Starting to listen for a connection..."
        connection, client_address = s.accept()
        print "Connection accepted from: " +  str(client_address)
        stopped = False
        while not stopped:
            data = connection.recv(4096)
            print "Received message: >" + data + "<"
            if data == "":
                break
            stopped = handle_request(data, connection)
    except socket.error, msg:
        print "Failed to create socket. Error code: " + str(msg[0]) + " , Error message: " + str(msg[1])
    except KeyboardInterrupt:
        print "KeyBoardInterrupt detected"
    finally:
        s.close()
        sys.exit(1)


def handle_request(data, connection):
    stopped = False
    
    renderDate = ""
    zoom = -1
    left = -200.0
    bottom = -200.0
    right = -200.0
    top = -200.0

    data_list = data.split('&')

    if data_list[0].lower() == "render":
        for i in range(1, len(data_list)):
            kv_set = data_list[i].split("=")
            if kv_set[0].lower() == "date":
                renderDate = kv_set[1]
            elif kv_set[0].lower() == "zoom":
                zoom = kv_set[1]
            elif kv_set[0].lower() == "left":
                left = kv_set[1]
            elif kv_set[0].lower() == "bottom":
                bottom = kv_set[1]
            elif kv_set[0].lower() == "right":
                right = kv_set[1]
            elif kv_set[0].lower() == "top":
                top = kv_set[1]
            else:
                print "message not according to protocol (unknown parameter) - ignored"
                send_error("Message not according to protocol (unknown parameter) - ignored", connection)
                stopped = True
                break

        if not stopped:
            try:
                print "Trying to render with: ", renderDate, zoom, left, bottom, right, top
                do_render(renderDate, zoom, left, bottom, right, top)
                send_done(connection)
            except ValueError as e:
                print e.message
                print "Not rendering this request..."
                send_error(e.message, connection)
    
    elif data_list[0] == "stop" or data_list[0] == "":
        stopped = True
        print "Received " + data_list[0] + ", stopping program..."
    else:
        print "message not according to protocol (unknown command) - ignored "
        send_error("Message not according to protocol (unknown command) - ignored", connection)
    return stopped


def send_done(connection):
    done_string = "done&"
    done_string += "Rendered successfully."
    connection.send(done_string)


def send_error(error, connection):
    error_string = "error&"
    error_string += error
    connection.send(error_string)

#################################################################################################
## database related
#################################################################################################


# Retrieves the connection info for the database from inc/datasource-settings.xml.inc
def getDatabaseSettings():
    index = 0

    with open(DATABASE_SOURCE, 'rt') as myfile:
        contents = myfile.read()

    word = "<Parameter name=\"password\">"
    dbpassword = readParameter(contents, word, "<")

    word = "<Parameter name=\"host\">"
    dbhost = readParameter(contents, word, "<")

    word = "<Parameter name=\"port\">"
    dbport = readParameter(contents, word, "<")

    word = "<Parameter name=\"user\">"
    dbuser = readParameter(contents, word, "<")

    word = "<Parameter name=\"dbname\">"
    dbname = readParameter(contents, word, "<")

    return [dbhost, dbport, dbname, dbuser, dbpassword]


# searches "text" for "name", then reads characters after "name", until it hits
# the character "exitSymbol", then returns read characters. "exitSymbol" itself 
# will not be returned
# raises ValueError if "name" or "end_symbol" after "name" was not found.
def readParameter(text, name, end_symbol):
    index = text.find(name) + len(name)

    if index < 0:
        print "inc/database-settings.xml.inc malformed (did not find " + name + ")"
        raise ValueError("inc/database-settings.xml.inc malformed (did not find " + name + ")")
    
    try:
        parameter = read2key(text, index, end_symbol)
    except ValueError:
        print "inc/database-settings.xml.inc malformed (did not find ending tag for " + name + ")"
        raise ValueError("inc/database-settings.xml.inc malformed (did not find ending tag for " + name + ")")
    return parameter


# returns a string containing all characters read in "text" from "index" to "end_symbol"
# raises a ValueError if "end_symbol" was not found
def read2key(text, index, end_symbol):
    word = ""
    while text[index] != end_symbol:
        word += text[index]
        if index > len(text):
            raise ValueError("end_symbol '" + end_symbol + "' not found")
        index += 1
    return word


# Changes the Prefix to "d_[renderDate]" for the osm.xml which is defined in inc/setting.xml.inc
def changePrefix(renderDate):
    stripDate = renderDate.replace('.', '')
    prefix = "<!ENTITY prefix \""
    manipulatedPrefix = "<!ENTITY prefix \"d_" + stripDate

    with open(PREFIX_SOURCE, 'rt') as myfile:
        contents = myfile.read()

    index = contents.find(prefix)
    index2 = contents.find("\">", index)

    contents = contents[:index] + manipulatedPrefix + contents[index2:]

    with open(PREFIX_SOURCE, 'wt') as myfile:
        myfile.write(contents)


#########################################################################################################
## Database: Create and Drop View
#########################################################################################################


def createViews(renderDate):
    stripDate = renderDate.replace('.', '')

    # Create names for views
    datePoint = "d_" + stripDate + "_point"
    dateLine = "d_" + stripDate + "_line"
    datePolygon = "d_" + stripDate + "_polygon"

    # Get database
    dbsettings = getDatabaseSettings()

    # Create views Queries
    pointView = "CREATE OR REPLACE VIEW " + datePoint + " as (SELECT * FROM planet_osm_point WHERE '"\
                + renderDate + "' BETWEEN valid_since AND valid_until);"
    lineView = "CREATE OR REPLACE VIEW " + dateLine + " as (SELECT * FROM planet_osm_line WHERE '"\
               + renderDate + "' BETWEEN valid_since AND valid_until);"
    polygonView = "CREATE OR REPLACE VIEW " + datePolygon + " as (SELECT * FROM planet_osm_polygon WHERE '"\
                  + renderDate + "' BETWEEN valid_since AND valid_until);"

    # Establish connection
    try:
        connection = psycopg2.connect(host=dbsettings[0], port=dbsettings[1], dbname=dbsettings[2], user=dbsettings[3],
                                      password=dbsettings[4])
        cursor = connection.cursor()

        # Create views
        cursor.execute(pointView)
        cursor.execute(lineView)
        cursor.execute(polygonView)
        connection.commit()

        # Close connection
        cursor.close()
        connection.close()
    except Exception as e:
        print "SQL-Error occurred: " + str(e) + " - closing connection..."
        try:
            cursor.close()
            connection.close()
        except:
            print "Connection already closed or never established."
        print "SQL-Error occurred, not executing..."
        raise Exception("SQL-Error occurred")


def dropViews(renderDate):
    stripDate = renderDate.replace('.', '')

    # Create names for views
    datePoint = "d_" + stripDate + "_point"
    dateLine = "d_" + stripDate + "_line"
    datePolygon = "d_" + stripDate + "_polygon"

    # Create view queries
    pointView = "DROP VIEW IF EXISTS " + datePoint + ";"
    lineView = "DROP VIEW IF EXISTS " + dateLine + ";"
    polygonView = "DROP VIEW IF EXISTS " + datePolygon + ";"

    # Get database
    dbsettings = getDatabaseSettings()

    # Establish connection
    try:
        connection = psycopg2.connect(host=dbsettings[0], port=dbsettings[1], dbname=dbsettings[2], user=dbsettings[3],
                                      password=dbsettings[4])
        cursor = connection.cursor()

        # Drop views
        cursor.execute(pointView)
        cursor.execute(lineView)
        cursor.execute(polygonView)
        connection.commit()

        # Close connection
        cursor.close()
        connection.close()
    except:
        print "SQL-Error occurred, closing connection..."
        try:
            cursor.close()
            connection.close()
        except:
            print "Connection already closed or never established."
        print "SQL-Error occurred, not executing..."
        raise Exception("SQL-Error occurred")


#########################################################################################################
## Rendering
#########################################################################################################

def validate_params(date, zoom, left, bottom, right, top):
    try:
        zoom = int(zoom)
        left = float(left)
        bottom = float(bottom)
        right = float(right)
        top = float(top)
    except ValueError:
        raise ValueError("One of the arguments was not correct, got: date: " + str(date) + ", zoom: " +
                         str(zoom) + ", left: " + str(left) + ", bottom: " + str(bottom) + ", right: " +
                         str(right) + ", top: " + str(top))

    # validate arguments
    try:
        datetime.datetime.strptime(date, "%d.%m.%Y")
    except ValueError:
        raise ValueError("Date seems to be incorrect (dd.mm.yyyy)")

    if zoom < 0 or zoom > 18:
        raise ValueError("Zoom level " + str(zoom) + " seems to be incorrect (0 - 18)")

    if left < -180 or left > 180:
        raise ValueError("First longitude seems to be incorrect (-180.0 - +180.0)")

    if bottom < -90 or bottom > 90:
        raise ValueError("First latitude seems to be incorrect (-90.0 - +90.0)")

    if right < -180 or right > 180:
        raise ValueError("Second longitude seems to be incorrect (-180.0 - +180.0)")

    if top < -90 or top > 90:
        raise ValueError("Second latitude seems to be incorrect (-90.0 - +90.0)")

    if left > right:
        raise ValueError("First longitude (left) needs to be smaller than the second longitude (right)")

    if bottom > top:
        raise ValueError("First latitude (bottom) needs to be smaller than the second latitude (top)")


def do_render(date, zoom, left, bottom, right, top):
    validate_params(date, zoom, left, bottom, right, top)

    """
    print "Starting with these arguments:"
    print "date=" + date + ", zoom=" + str(zoom) + ", left=" + str(left) + ", bottom="\
        + str(bottom) + ", right=" + str(right) + ", top=" + str(top) + ", port=" + str(port)
    """

    bbox = (left, bottom, right, top)

    mapfile = os.path.join(DIRNAME, "osm.xml")

    output_directory = TILE_DIR + date.replace('.', '-') + "/"

    changePrefix(date)

    createViews(date)

    render_tiles(bbox, mapfile, output_directory, zoom, zoom, "Map")

    dropViews(date)


#########################################################################################################
## Use Script from CLI
#########################################################################################################

def printUsage():
    print """
Usage:
python generate_view_tiles.py [-options] [arguments]

You can either 
[1] : give all [-d -z -l -b -r -t] options and arguments to render once and end the script
OR
[2} : give just [-p] to start this script listening on port [argument] for incoming render requests

Options:
    -h or --help:   Show this message
    
    -d ir --date:   date to render map for (required for [1])
                    format: dd.mm.yyyy
    -z or --zoom:   zoom to render tiles in (required for [1])
                    format: Integer from 0 to 18
    -l or --left:   minimal longitude of bounding box to render (required for [1])
                    format: Floating Point from -180.0 to 180.0
    -b or --bottom: minimal latitude of bounding box to render (required for [1])
                    format: Floating Point from -90.0 to 90.0
    -r or --right:  maximal longitude of bounding box to render (required for [1])
                    format: Floating Point from -180.0 to 180.0
    -t or --top:    maximal latitude of bounding box to render (required for [1])
                    format: Floating Point from -90.0 to 90.0
    
    -p or --port:   port to listen on for incoming render requests (required for [2])
    """


if __name__ == "__main__":
    if len(sys.argv) == 1:
        print "No arguments given, see --help for usage."
        sys.exit(1)

    renderDate = ""
    zoom = -1
    left = -200.0
    bottom = -200.0
    right = -200.0
    top = -200.0
    port = -1

    # get options
    try:
        options, arguments = getopt.getopt(sys.argv[1:], "hd:z:l:b:r:t:p:",
                                           ["--help", "date=", "zoom=", "left=", "bottom=", "right=", "top=", "port="])
    except getopt.GetoptError:
        printUsage()
        sys.exit(1)

    for opt, arg in options:
        if opt in ("-d", "--date"):
            renderDate = arg
        elif opt in ("-z", "--zoom"):
            zoom = int(arg)
        elif opt in ("-l", "--left"):
            left = float(arg)
        elif opt in ("-b", "--bottom"):
            bottom = float(arg)
        elif opt in ("-r", "--right"):
            right = float(arg)
        elif opt in ("-t", "--top"):
            top = float(arg)
        elif opt in ("-p", "--port"):
            port = int(arg)
        elif opt in ("-h", "--help"):
            printUsage()
            sys.exit(0)
        else:
            print "See help (-h or --help) for usage."
            sys.exit(1)

    # Check which mode should be used
    # (-p : start listening for render requests,
    # other options : render once with given arguments)
    if port > -1:
        if renderDate != "" or zoom != -1 or left != -200.0 or bottom != -200.0 or right != -200.0 or top != -200.0:
            print "You can only use either --port (help: [2]) OR other arguments (help: [1])."
            print "See --help for usage."
            sys.exit(1)
        else:
            doConnection(port)
    elif port == -1:
        if renderDate != "" and zoom != -1 and left != -200.0 and bottom != -200.0 and right != -200.0 and top != -200.0:
            do_render(renderDate, zoom, left, bottom, right, top)
        else:
            print "All parameters must be given to render from CLI (-d -z -l -b -r -t)."
            print "See -h (--help) for usage."
            sys.exit(1)
    else:
        print "Port needs to be positive."
        sys.exit(1)

    sys.exit(0)
