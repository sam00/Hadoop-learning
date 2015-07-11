# Hadoop-learning

>>MapReduce Filter

MR job for Filter the records for tilte with keyword “night” O/P : Output record which has only records containg keyword "night" in title.

>>Job Chaining

Job Chaining: Filter records based on title for Job A and then take that output as input for Job B and filter again for title:

>>Join

Perform Join using MapReduce

>>Unique visitor count

Unique visitor count: logs (ipaddress, website).eg I/P => usera - google; usera - google ; userb - wiki ; usera - wiki O/P => google - 1; wiki - 2

>>Join using distributed cache

Perform a join on two tables using a distributed cache approach.

>>Temperature Exercise

To find the Maximum temperature for the years from the weather data set. I/P (Example) =>
1942 128 1 1940 136 1 1956 124 1 1946 128 1 1940 136 1 1956 154 1 1942 138 1 1940 116 1 1954 124 1 1942 128 1 1945 152 1 1956 124 1 1941 111 1 1940 136 1 1961 164 1

O/P => 1940 136 1941 111 1942 138 1945 152 1946 128 1954 124 1956 154 1961 164

>>Temperature Exercise checks with real data
 
To find the Maximum temperature for the years from the weather data set. I/P (Example) =>
1853	21.2
1854	21.7
1855	22.8
1856	22.7
1857	22.8
1858	23.6
1859	25.1
1860	19.3
1861	21.6
1862	20.0
1863	21.8
1864	22.8
1865	22.3
1866	21.9
1867	21.5
1868	25.8
1869	24.1
1870	24.9
1871	23.8
1872	23.7
1873	22.6
1874	25.3
1875	21.9
1876	24.5
1877	22.2
1878	22.2
1879	18.7
1880	20.5
1881	22.7
1882	20.0
1883	21.0
1884	23.9
1885	22.7
1886	21.8

