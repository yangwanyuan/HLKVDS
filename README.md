# HLKVDS is a Hybrid Layout Key Value Data Store

1. build the HLKVDS with
		
		$ make

2. run unit test with

		$ ./install_deps.sh
		$ make test
		$ cd test 
		$ ./test.sh

3. There some tools to start

	Create data store with
	
		$ ./tool/CreateDb /dev/sd*
		
	Open and operate to data store example with
		
		$ ./tool/ExampleKV /dev/sd*	
		
	Get all data on the device with
		
		$ ./tool/LoadDB /dev/sd*

4. There is a benchmark tool to test the performance

		$ ./tool/Benchmark write|overwrite|read -f dbfile -s db_size -n num_records -t thread_num -seg segment_size(KB)
