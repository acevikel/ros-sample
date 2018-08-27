# ros-sample
Path node for following virtual path for an industrial self driving vehicle. 
This service listens the active map ID and publishes related no-go and path maps. If the path id is successfully found on the database, the service returns True or False accordingly.
Path map is used as a static layer for the costmap which effectively favors the pre-planned path in
the global planner.
Nogo map is the virtual bound of the vehicle which is used in both local and global planners. 
It is also possible to add charging station locations with orientation and include them into nogo map in order to avoid them during path planning, yet this feature is currently disabled.