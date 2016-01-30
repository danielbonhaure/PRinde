
impute_mf <- function(datosEstacion, variable, estaciones, missingIndexes, registrosVecinos, vecinos.data, max_parallelism=1) {
    impData <- as.tbl(datosEstacion[-missingIndexes,]) %>%
        sample_n(0.5 * nrow(datosEstacion)) %>%
        bind_rows(as.tbl(datosEstacion[missingIndexes,])) %>%
        arrange(fecha)
    impData <- impData %>% select(one_of('omm_id', 'fecha', 'tmax', 'tmin', 'prcp', 'helio', 'nub'))
    missingIndexesSubset <- which(is.na(impData[, variable]))

    used_neighbors <- 0

    for(vecino in vecinos.data$omm_id) {
        datosVecino <- filter(registrosVecinos, omm_id == vecino)
        NAprop <- (sum(is.na(datosVecino[, variable])) + nrow(datosEstacion)-nrow(datosVecino)) / nrow(datosEstacion)

        # Máximo cuatro vecinos con datos.
        if(used_neighbors >= 3) next;

        # Para ser usado, el vecino tiene que tener menos de 20% de faltantes.
        if(NAprop > 0.2) next;
        used_neighbors <- used_neighbors + 1

        impData <- impData %>% dplyr::left_join(
            datosVecino %>%
                select(one_of(c(variable, "fecha"))) %>%
                rename_(.dots=setNames(c(variable), paste0(vecino, '.', variable))),
            by='fecha')
    }
    impData <- impData %>% select(-one_of('omm_id', 'fecha'))
    impData <- as.data.frame(impData)

    if(max_parallelism > 1) {
        if(require(doMC)) {
            registerDoMC(min(ncol(impData) - 2, max_parallelism))
            impData <- missForest(impData, ntree=100, parallelize='forests')$ximp
        } else {
            impData <- missForest(impData, ntree=100)$ximp
        }
    } else {
        # Repeat this code because R doesn't behave too well with "and" conditions when evaluating if's.
        impData <- missForest(impData, ntree=100)$ximp
    }


    datosEstacion[missingIndexes, variable] <- impData[missingIndexesSubset, variable]
    return(datosEstacion)
}


impute_idw <- function(datosEstacion, variable, estaciones, missingIndexes, registrosVecinos, vecinos.data, max_parallelism=1) {
    idw.grid <- estaciones[estaciones$omm_id == estacion, c('x', 'y')]
    coordinates(idw.grid) <- ~x+y

    idwFormula <- formula(x=paste0(variable, "~1"))

    values <- sapply(missingIndexes, FUN=function(x) {
        imp.date <- datosEstacion[x, 'fecha']
        neighborData <- data.frame(registrosVecinos %>% filter(fecha == imp.date))

        if(nrow(neighborData) == 0) return(NA);


        coordinates(neighborData) <- ~x+y
        # Filtrar valores NA.
        neighborData <- neighborData[ !is.na(neighborData@data[, variable]), ]

        impute <- idw(idwFormula, neighborData, idp=2, idw.grid, debug.level=0)
        impute$var1.pred
    })

    datosEstacion[missingIndexes, variable] <- values

    missingIndexes <- which(is.na(datosEstacion[, variable]))

    if(length(missingIndexes) > 0) {
        print("Some indexes weren't estimated with IDW, trying with MF.")
        datosEstacion <- impute_mf(datosEstacion, variable, estaciones, missingIndexes, registrosVecinos, vecinos.data, max_parallelism)
    }
    return(datosEstacion)
}