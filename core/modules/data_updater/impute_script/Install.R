options(repos=c(CRAN="http://lib.stat.cmu.edu/R/CRAN/"))

suppressPackageStartupMessages(if(!require('lazyeval')) { install.packages('lazyeval', dependencies=TRUE); require('lazyeval') })
suppressPackageStartupMessages(if(!require('sirad')) { install.packages('sirad', dependencies=TRUE); require('sirad') }) # sudo apt-get install udunits-bin libudunits2-0 libudunits2-dev
suppressPackageStartupMessages(if(!require(missForest)) { install.packages("missForest", dependencies=TRUE); require(missForest) })
suppressPackageStartupMessages(if(!require(doMC)) { install.packages("doMC", dependencies=TRUE); require(doMC) })
suppressPackageStartupMessages(if(!require(gstat)) { install.packages("gstat"); require(gstat) }) # sudo apt-get install libgeos-dev
suppressPackageStartupMessages(if(!require(geosphere)) { install.packages("geosphere"); require(geosphere) })
suppressPackageStartupMessages(if(!require(rgdal)) { install.packages("rgdal"); require(rgdal) });  # sudo apt-get install libproj-dev libgdal-dev
suppressPackageStartupMessages(if(!require(dplyr)) { install.packages("dplyr", dependencies=TRUE); require(dplyr) });
suppressPackageStartupMessages(if(!require(optparse)) { install.packages("optparse", dependencies=TRUE); require(optparse) });
