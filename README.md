easy-redis-data
===============

A tool to read/write data(java object) in redis, and it's based on <a href="https://github.com/xetorthio/jedis">jedis</a>

Store data object in xml format in redis, can use compress option when storing.

Some test results is interesting:<br/>
Length of compressed content(20 kbyte before compressed) in gzip is about half of LZF.<br>
Time cost of Compressing 20 kbyte in gzip is about 3 times of LZF.
