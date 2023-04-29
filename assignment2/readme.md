Map-Reduce:
First map-reduce:
	Count the occurences in each corpus for each 3-gram.
	
	input:
	Key: <lineId> , Value : <3-gram>
	
	output:
	Key: <3-gram> , Value : <count in corpus 0 , count in corpus 1>

Second map-reduce:
	Calculates the probabilty for each r.

	Input:
	Key: <3-gram> , Value : <count in corpus 0 , count in corpus 1>

	Output:
	key: <3-gram,probability> , value : <probability>
	Adding probability to key for map-reduce3 sorting 

	Mapper:
	Creates 3 new rows from each row in first map-reuce output:
	1. calculation of N_r0, Tr_01 for the current 3-gram
	2. caluculation of N_r1, Tr10 for the current 3-gram
	3. the total count of the current 3-gram in both corpuses
	 
	Combiner:
	Combine localy only N_r0,N_r1,Tr_01,Tr10 values.
	Probability cacluation can accur only by the reducer after we got N_r0,N_r1,Tr_01,Tr10 values from all computers.

	Reducer:
	Key sorting implemntation: sort by r value and if r value matched sort by boolean value.

	Use r and false tag to calculate all N_r0,N_r1,T_r01,Tr10 values for the same r
	Use r and true tag to create each 3-gram with total count r probabilty with N_r0,N_r1,Tr_01,Tr10 final values
	
	
Third map-reduce:
	Used only for the final sorting,
        <w1,w2> ascending and probability descending with shuffle and sort from mapper to reducer
	
	Input:
	Key: <3-gram,probability> , Value : <probability>
	Output:
	Key: <3-gram,probability> , Value : <probability>	


run instructions:
	create a bucket and upload to it the heb-stopwords.txt included and change file type to utf8
	in buckeat create empty log folder
	in main.jav:
		lines 21,23,27,29,33,35,76 change bucket name
	in mapperreducer1.java
		line 70 change bucket name
	package the code and upload to all parts jar files to bucket