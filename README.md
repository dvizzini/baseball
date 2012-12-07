Baseball
========

This project contains the com.danielvizzini.baseball package, which calculates familiar batting statistics normalized for that half-inning. The three statistics calculated are:

* __pitches_faced_by_batter_in_half_inning / total_pitches_in_half_inning__
* __rbis_by_batter_in_half_inning / total_rbis_in_half_inning__
* __bases_obtained_by_batter_in_half_inning / total_bases_obtained_in_half_inning__

The idea being that the quality of pitches faced by all batter's is roughly equivilent throughout a half-inning, so that good performance in the face of weak pitching should count less than good performance in the face of strong pitching. Limitations to this approach are discussed below.

Note that only batter's with at least 100 atbats are tabulated.

Running the algorithm
---------------------
It is recommended that you run this algorithm on a fresh instance of [Cloudera's Sample Hadoop CDH4 VM](http://blog.cloudera.com/blog/2012/08/hadoop-on-your-pc-clouderas-cdh4-virtual-machine/) 

Initial setup can be accomplished by running the following script:

  ./setup.sh

A fresh jar can be created and executed with the following script
  ./execute.sh [output\_file]

Prerequisites
-------------
This system was deployed on [Cloudera's Sample Hadoop CDH4 VM](http://blog.cloudera.com/blog/2012/08/hadoop-on-your-pc-clouderas-cdh4-virtual-machine/). The __pom.xml__ file is only included for convenient IDE use. No tests have been made using it to construct a build path.

Input Files
-----------
Input files ere the [2012 retrosheet.org event files](http://www.retrosheet.org/game.html). An explanation of this file is [here](http://www.retrosheet.org/GameFiles.pdf).

Limiations
----------
One obvious limitation is that in an inning with multiple pitchers different the quality normalization is compromized. Another is that the statistics normalize for a batter's fellow teammates in addition to his opposing pitcher. A good batter on a bad-hitting team may have better statistics than a better batter on a good-hitting team.
