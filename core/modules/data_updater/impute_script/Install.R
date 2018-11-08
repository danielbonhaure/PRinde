options(repos=c(CRAN="https://cran.r-project.org/"))

suppressPackageStartupMessages(if(!require('lazyeval')) { install.packages("lazyeval", quiet=TRUE); require('lazyeval') })  # sudo apt install libssl-dev
suppressPackageStartupMessages(if(!require('sirad')) { install.packages("sirad", quiet=TRUE); require('sirad') })
suppressPackageStartupMessages(if(!require('missForest')) { install.packages("missForest", quiet=TRUE); require('missForest') })
suppressPackageStartupMessages(if(!require('doMC')) { install.packages("doMC", quiet=TRUE); require('doMC') })
suppressPackageStartupMessages(if(!require('rgeos')) { install.packages("rgeos", quiet=TRUE); require('rgeos') })  # sudo apt install libgeos++-dev
suppressPackageStartupMessages(if(!require('gstat')) { install.packages("gstat", quiet=TRUE); require('gstat') })
suppressPackageStartupMessages(if(!require('geosphere')) { install.packages("geosphere", quiet=TRUE); require('geosphere') })
suppressPackageStartupMessages(if(!require('rgdal')) { install.packages("rgdal", quiet=TRUE); require('rgdal') });  # sudo apt install libgdal-dev
suppressPackageStartupMessages(if(!require('optparse')) { install.packages("optparse", quiet=TRUE); require('optparse') });
suppressPackageStartupMessages(if(!require('xts')) { install.packages("xts", quiet=TRUE); require('xts'); })
suppressPackageStartupMessages(if(!require('dplyr')) { install.packages("dplyr", quiet=TRUE); require('dplyr') });
