#!/usr/bin/env python
import rospy
from std_msgs.msg import String
from nav_msgs.msg import Path, OccupancyGrid
from geometry_msgs.msg import PoseWithCovarianceStamped, PointStamped, PoseStamped
from sdv_scripts.srv import PathMapRequest, PathMap, PathMapResponse
from tf import transformations as transform
import pymysql.cursors
from skimage import draw
import numpy as np
import math
import signal
import json
import time



st_overlay = [(0.5,0.7),(-0.7,0.7),(-0.7,-0.7),(0.5,-0.7)]
tfed_st_overlay = 0


class MapNode(object):

    def __init__(self):
        self.initial_run = True
        self.last_path_id = None
        self.path_returned = False
        self.nogo_returned = False
        self.path_map_recceived = False
        self.last_path_id = 0
        self.active_map = None
        self.path_map = OccupancyGrid()
        self.nogo_map = OccupancyGrid()
        self.local_nogo_map = OccupancyGrid()
        self.connection = pymysql.connect(host='127.0.0.1',
                                    user='username',
                                    password='pass',
                                    db='sdvDB',
                                    charset='utf8mb4',
                                    cursorclass=pymysql.cursors.DictCursor)

        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)


    @staticmethod
    def signal_handler(signal, frame):
        print "\nClosing Map Node.."
        rospy.signal_shutdown("Done")


    def connect_database(self):
        self.connection = pymysql.connect(host='127.0.0.1',
                                          user='username',
                                          password='pass',
                                          db='sdvDB',
                                          charset='utf8mb4',
                                          cursorclass=pymysql.cursors.DictCursor)


    def active_map_cb(self, data):
        self.active_map = int(data.data)
        print "Active map set to {0}".format(self.active_map)


    def get_path_data(self, path_id):
        try:
            self.connect_database()
            with self.connection.cursor() as cursor:
                query = "SELECT PATH_INTERPOLATE_ARRAY FROM `PATHS` where PATH_ID =%s" % (str(path_id))
                cursor.execute(query)
                result = cursor.fetchone()
                list_in =  json.loads(result["PATH_INTERPOLATE_ARRAY"])                
                x,y = [], []                
                for elem in list_in:
                    vals = elem.split(",")
                    x.append(float(vals[0]))
                    y.append(float(vals[1]))
        except Exception as ee:
            print "Exception raised while getting path data.\n", ee
            x, y = None, None
        finally:
            self.connection.close()
        return x, y


    @staticmethod
    def rotate_point(point, theta, x, y):
        """
        Method for transforming station points according to orientation
        """
        r_rot = transform.rotation_matrix(theta, [0, 0, 1])
        vect = [point[0], point[1], 0, 1]
        trans = np.dot(r_rot, vect)
        pt_out = (trans[0] + x, trans[1] + y)
        return pt_out


    def get_allowed_zone_from_db (self):
        """
        return [] if no allowed zone, else points
        return data structure = [[[x1,y1],[x2,y2]]...]
        """
        zone = []
        if not self.active_map:
            print "Not active map published at this point in time, returning hard coded zones"
            #DISABLED BECAUSE NOT NEEDED
            #return [((0,0),(3,3)),((23.1,9.17),(7.37,-6.58))]
            #return [[[40, -150], [39.1, -158]]]
            return []

        try:
            self.connect_database()
            with self.connection.cursor() as cursor:
                query = "SELECT MAP_ZONE FROM `MAP_ZONE` where MAP_ID =%s" % (str(self.active_map))
                cursor.execute(query)
                result = cursor.fetchone()
                if result: zone = json.loads(result["MAP_ZONE"])
        except Exception as ee:
            print "Exception raised while getting path data.\n", ee            
            zone = []  
        finally:
            self.connection.close()
        return zone


    @staticmethod  
    def get_station_cords(pose):
        """
        Method for getting euler yaw from quaternion pose in json
        """
        pose_dict = json.loads(pose)
        x = float(pose_dict["position"]["x"])
        y = float(pose_dict["position"]["y"])
        ox = float(pose_dict["orientation"]["x"])
        oy = float(pose_dict["orientation"]["y"])
        oz = float(pose_dict["orientation"]["z"])
        ow = float(pose_dict["orientation"]["w"])
        yaw =  transform.euler_from_quaternion([ox,oy,oz,ow])[2]
        return (x, y, yaw)


    def get_station_poses_from_db(self):
        """
        return [] if no station, else points
        return data structure = [(x,y,yaw)...]
        """
        if not self.active_map:
            print "Not active map published at this point in time, returning hard coded stations"
            return [(18.8,1.31,math.radians(0)),(77.9,-26.1,math.radians(0))]

        return_list = []
        try:
            self.connect_database()
            with self.connection.cursor() as cursor:
                query = "SELECT STATION_POSE FROM `STATIONS` where STATION_MAP_ID =%s" % (str(self.active_map))
                cursor.execute(query)
                result = cursor.fetchall()
                if result == () : return []
                for res in result:
                    if res["STATION_POSE"]:
                        return_list.append(self.get_station_cords(res["STATION_POSE"]))
        except Exception as ee:
            print "Exception raised while getting path data.\n", ee                        
            return_list = []
        finally:
            self.connection.close()
               
        return return_list


    def wait_costmaps(self, timeout=20, period=0.1):
        """
        Utility function for waiting costmaps
        """
        mustend = time.time() + timeout
        while time.time() < mustend:
            if (self.path_returned and self.nogo_returned) : return True
            time.sleep(period)
        return False


    def wait_input_map(self, timeout=60, period=0.1):
        """
        Utility function for waiting input map
        """
        mustend = time.time() + timeout
        while time.time() < mustend:
            if self.path_map_received: 
                self.total_map_wait = 0 
                return True
            self.total_map_wait +=1
            time.sleep(period)
            print "Waited {0} seconds for initial map".format(self.total_map_wait)
        self.total_map_wait = 0 
        print "Not received map within {0} seconds. Terminating".format(timeout)
        return False


    def nogo_cb(self, data):
        """
        Callback that receives no-go map for verification.
        """        
        self.nogo_returned = True
        print "Received No-Go map successfully."


    def path_cb(self, data):
        """
        Callback that receives path map for verification.
        """        
        self.path_returned = True
        print "Received Path Map successfully."

        
    def path_request_handler(self, data):
        """
        Method that creates and publishes the no-go and path maps
        """                    
        incoming = time.time()
        self.path_returned = False
        self.nogo_returned = False
        path_id = data.path_id
        print "Incoming request : Cur Path :", path_id,"  Old Path : ", self.last_path_id

        if path_id == self.last_path_id:
            print "Not changing path since it was already the active map"
            return PathMapResponse(True)

        if not self.wait_input_map(): return PathMapResponse(False)   

        try:
            x_list, y_list = self.get_path_data(path_id)
            self.last_path_id = path_id
        except:
            return PathMapResponse(False)

        
        path_map_array = np.full((self.map_height * self.map_width, ), 60, dtype=int)
        path_map_array = np.reshape(path_map_array, (self.map_width, self.map_height), order="F")          

        for i in range(0, len(x_list), 1):
            x = int(abs(self.orig_x - x_list[i]) / self.map_res)
            y = int(abs(self.orig_y - y_list[i]) / self.map_res)

            ri, ci = draw.circle(x, y, radius=5, shape=path_map_array.shape)
            path_map_array[ri, ci] = 0
  

        self.path_map.data = np.reshape(path_map_array, (self.map_width * self.map_height,), order="F")
        self.path_map.header.frame_id = 'map'
        self.path_map_pub.publish(self.path_map)
        print "Path map published"

        nogo_map_array = np.full((self.map_width * self.map_height,), 100, dtype=int)
        nogo_map_array = np.reshape(nogo_map_array, (self.map_width, self.map_height), order="F")

        for i in range(0, len(x_list), 8):
            x = int(abs(self.orig_x - x_list[i]) / self.map_res)
            y = int(abs(self.orig_y - y_list[i]) / self.map_res)
            ri, ci = draw.circle(x, y, radius=100, shape=nogo_map_array.shape)
            nogo_map_array[ri, ci] = 0
        
        self.nogo_map.data = np.reshape(nogo_map_array, (self.map_width * self.map_height,), order="F")
        self.nogo_map.header.frame_id = 'map'
        self.nogo_map_pub.publish(self.nogo_map)
        
        ###zones
        if False:   # DISABLED SINCE CURRENTLY NOT NEEDED
            local_nogo_map_array = []
            allowed_zone = self.get_allowed_zone_from_db()
            if allowed_zone:
                local_nogo_map_array = np.zeros((self.map_width * self.map_height, ), dtype=int)
                local_nogo_map_array = np.reshape(local_nogo_map_array, (self.map_width, self.map_height), order="F")
                for p1, p2 in allowed_zone:
                    p1_x = int(abs(self.orig_x - p1[0]) / self.map_res)
                    p1_y = int(abs(self.orig_y - p1[1]) / self.map_res)
                    p2_x = int(abs(self.orig_x - p2[0]) / self.map_res)
                    p2_y = int(abs(self.orig_y - p2[1]) / self.map_res)
                    r = np.array([p1_x, p2_x, p2_x, p1_x])
                    c = np.array([p1_y, p1_y, p2_y, p2_y])
                    rr, cc = draw.polygon(r, c, shape=local_nogo_map_array.shape)
                    local_nogo_map_array[rr, cc] = 100

            charge_stats = self.get_station_poses_from_db()
            if charge_stats:
                if not local_nogo_map_array:
                    local_nogo_map_array = np.zeros((self.map_width * self.map_height, ), dtype=int)
                    local_nogo_map_array = np.reshape(local_nogo_map_array, (self.map_width, self.map_height), order="F")
                
                for cur_stat in charge_stats:
                    x_c, y_c, yaw = cur_stat
                    tfed_st_overlay = [self.rotate_point(cur,yaw,x_c,y_c) for cur in st_overlay]
                    x_list = [int(abs(self.orig_x - elem[0]) / self.map_res) for elem in tfed_st_overlay]
                    y_list = [int(abs(self.orig_y - elem[1]) / self.map_res) for elem in tfed_st_overlay]
                    r = np.array(x_list)
                    c = np.array(y_list)
                    rr, cc = draw.polygon(r, c, shape=local_nogo_map_array.shape)
                    local_nogo_map_array[rr, cc] = 100
            if allowed_zone or charge_stats:
                self.local_nogo_map.data = np.reshape(local_nogo_map_array, (self.map_width * self.map_height,), order="F")
                self.local_nogo_map.header.frame_id = 'map' 
                self.local_nogo_map_pub.publish(self.local_nogo_map)        

        print "{} seconds elapsed.".format(time.time() - incoming)

        if self.wait_costmaps():
            return PathMapResponse(True)
        else:
            print "Waited maps more than 20s, timeouted"
            return PathMapResponse(False)

        
    def map_cb(self, orig_map): 
        """
        Callback that gets the map data.
        """           
        self.map_width = orig_map.info.width
        self.map_height = orig_map.info.height
        self.map_res = orig_map.info.resolution
        self.orig_x = orig_map.info.origin.position.x
        self.orig_y = orig_map.info.origin.position.y

        self.path_map.info = orig_map.info
        self.nogo_map.info = orig_map.info
        self.local_nogo_map.info = orig_map.info

        self.path_map_received = True

        if self.initial_run:
            
            self.path_map_pub.publish(orig_map)
            self.nogo_map_pub.publish(orig_map)
            self.local_nogo_map_pub.publish(orig_map)

        self.initial_run = False
        print "Received map successfully."


    def run(self):
        rospy.init_node("mapNode")
        self.path_id_pub = rospy.Publisher('path_id', String, queue_size=10)
        self.nogo_map_pub = rospy.Publisher('nogo_map', OccupancyGrid, queue_size=10, latch=True)
        self.path_map_pub = rospy.Publisher('path_map', OccupancyGrid, queue_size=10, latch=True)
        self.local_nogo_map_pub = rospy.Publisher('local_nogo_map', OccupancyGrid, queue_size=10, latch=True)
        rospy.Subscriber("/active_map", String, self.active_map_cb)
        rospy.Subscriber("map", OccupancyGrid, self.map_cb)
        rospy.Subscriber("nogo_map", OccupancyGrid, self.nogo_cb)
        rospy.Subscriber("path_map", OccupancyGrid, self.path_cb)
        rospy.Service('set_path_id', PathMap, self.path_request_handler)
        rospy.spin()


if __name__ == "__main__":
        MP = MapNode()
        MP.run()        

