options(repos=c(CRAN="https://cran.r-project.org/"))

suppressPackageStartupMessages(if(!require('lazyeval')) { install.packages("lazyeval", dependencies=TRUE, quiet=TRUE); require('lazyeval') })  # sudo apt install libssl-dev
suppressPackageStartupMessages(if(!require('sirad')) { install.packages("sirad", dependencies=TRUE, quiet=TRUE); require('sirad') })
suppressPackageStartupMessages(if(!require('missForest')) { install.packages("missForest", dependencies=TRUE, quiet=TRUE); require('missForest') })
suppressPackageStartupMessages(if(!require('doMC')) { install.packages("doMC", dependencies=TRUE, quiet=TRUE); require('doMC') })
suppressPackageStartupMessages(if(!require('rgeos')) { install.packages("rgeos", quiet=TRUE); require('rgeos') })  # sudo apt install libgeos++-dev
suppressPackageStartupMessages(if(!require('gstat')) { install.packages("gstat", quiet=TRUE); require('gstat') })
suppressPackageStartupMessages(if(!require('geosphere')) { install.packages("geosphere", quiet=TRUE); require('geosphere') })
suppressPackageStartupMessages(if(!require('rgdal')) { install.packages("rgdal", quiet=TRUE); require('rgdal') });  # sudo apt install libgdal-dev
suppressPackageStartupMessages(if(!require('optparse')) { install.packages("optparse", dependencies=TRUE, quiet=TRUE); require('optparse') });
suppressPackageStartupMessages(if(!require('xts')) { install.packages("xts", quiet=TRUE); require('xts'); })
suppressPackageStartupMessages(if(!require('dplyr')) { install.packages("dplyr", dependencies=TRUE, quiet=TRUE); require('dplyr') });
