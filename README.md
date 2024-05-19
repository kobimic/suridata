# Targil Anak Gamad

## there are 2 scripts
1. single process,  is running in a single process
it loads the json data, filter dup, shuffle it, and then create pairs of
(A,B),(B,C)....(X,Z),(Z,A)
meaning every person will be both anak and gamad and the last pair is circular pointing back to begining

2. multi process, get the data before running the subprocess,
   there is limitation on when the split of data will create a process with a single person, this will crash, so number of records must be 2 or more in each process
   if a process has a single record it cannot pair
   
3. the multi process has some complexity due to an edge case of 4 people and 2 processes, this will violate the rule of 2 people cannot be paired in reverse, so a post merge function is generating the pairs so even 4 people and 2 process will generate a result

   

   
