options(repos=c(CRAN="http://cran.stat.ucla.edu/"))

suppressPackageStartupMessages(if(!require('lazyeval')) { install.packages("lazyeval", dependencies=TRUE, quiet=TRUE); require('lazyeval') })  # sudo apt install libssl-dev
suppressPackageStartupMessages(if(!require('sirad')) { install.packages("sirad", dependencies=TRUE, quiet=TRUE); require('sirad') })
suppressPackageStartupMessages(if(!require('missForest')) { install.packages("missForest", dependencies=TRUE, quiet=TRUE); require('missForest') })
suppressPackageStartupMessages(if(!require('doMC')) { install.packages("doMC", dependencies=TRUE, quiet=TRUE); require('doMC') })
suppressPackageStartupMessages(if(!require('gstat')) { install.packages("gstat", dependencies=TRUE, quiet=TRUE); require('gstat') })
suppressPackageStartupMessages(if(!require('geosphere')) { install.packages("geosphere", dependencies=TRUE, quiet=TRUE); require('geosphere') })
suppressPackageStartupMessages(if(!require('rgdal')) { install.packages("rgdal", dependencies=TRUE, quiet=TRUE); require('rgdal') });  # sudo apt install libgdal-dev
suppressPackageStartupMessages(if(!require('optparse')) { install.packages("optparse", dependencies=TRUE, quiet=TRUE); require('optparse') });
suppressPackageStartupMessages(if(!require('xts')) { install.packages("xts", dependencies=TRUE, quiet=TRUE); require('xts'); })
suppressPackageStartupMessages(if(!require('dplyr')) { install.packages("dplyr", dependencies=TRUE, quiet=TRUE); require('dplyr') });
