## @knitr Init
rm(list=objects()); invisible(gc())

getScriptPath <- function(){
    cmd.args <- commandArgs()
    m <- regexpr("(?<=^--file=).+", cmd.args, perl=TRUE)
    script.dir <- dirname(regmatches(cmd.args, m))
    if(length(script.dir) == 0) return(".")
    if(length(script.dir) > 1) stop("can't determine script dir: more than one '--file' argument detected")
    return(script.dir)
}

setwd(getScriptPath())

# writeLines(paste('getwd(): ', getwd(), collapse = '\n\n'))

source('Install.R')
source('db/PostgreSQL/PostgreSQL.r')
source('db/Queries.R')
source('Estimar.R')
source('Utils.R')
source('Impute.R')

exit_status <- 0
errors <- c()

option_list = list(
    optparse::make_option(c("-s", "--stations"), action="store", default="", type='character', help="Comma separated list of weather stations ids that should be imputed."),
    optparse::make_option(c("-d", "--database"), action="store", default="crcsas", type='character', help="Weather database name"),
    optparse::make_option(c("-c", "--host"), action="store", default="localhost", type='character', help="Weather database host"),
    optparse::make_option(c("-u", "--user"), action="store", default="postgres", type='character', help="Weather database user name"),
    optparse::make_option(c("-p", "--port"), action="store", default=5432, type='integer', help="Weather database port"),
    optparse::make_option(c("-w", "--password"), action="store", default=NULL, type='character', help="Weather database password"),
    optparse::make_option(c("-m", "--parallelism"), action="store", default=1, type='integer', help="Max parallel tasks.")
)
opt = optparse::parse_args(optparse::OptionParser(option_list=option_list))

###########################
# opt$stations <- '87649,87270,87688,87497,87467,87534,87453,87374,87349,87544,87548,87645,87550,87637'
# opt$stations <- '87374,87349,87544,87548,87645,87550,87637'
# opt$stations <- '9987001'
# opt$host <- '127.0.0.1'
# opt$port <- 5433
# opt$database <- 'crcsas'
# opt$user <- 'postgres'
# opt$password <- 'rinde'
# opt$parallelism <- 4
###########################

stations <- as.integer(unlist(strsplit(opt$stations, ',')))
stations <- stations[!is.na(stations)]

stopifnot(length(stations) > 0)

# Creamos un array con los OMM ID's de las estaciones sobre las que queremos estimar.
estacionesID <- stations

# Creamos la conexión con la base de datos.
conexion <- pg_connect(user=opt$user, host=opt$host, dbname=opt$database, port=opt$port, password=opt$password)

srad_parameters <- list(
    'PY' = list(
        'bc' = c(A = 0.714, B = 0.007, C = 2.26),
        'svk' = c(A = 0.095, B = 0.345, C = -1.04),
        'ap' = c(A = 0.523, B = 0.232)
    ),
    'AR' = list(
        'ap' = c(A = 0.58, B = 0.2),
        'bc' = c(A = 0.69, B = 0.02, C = 2.12),
        'svk' = c(A = 0.06, B = 0.47, C = 0.8)
    )
)

tryCatch({
    # Obtenemos los datos de las estaciones.
    rs.estacion <- RPostgreSQL::dbSendQuery(conexion, paste0("SELECT * FROM estacion e LEFT JOIN institucion i ON e.institucion_id = i.id WHERE e.omm_id IN (", paste(estacionesID, collapse=','), ')'))
    estaciones <- RPostgreSQL::fetch(rs.estacion, n=-1)

    stopifnot(nrow(estaciones) > 0)

    # Calculamos las coordenadas en Gauss Kruger de cada estación.
    GK.coords <- Gauss.Kruger.coordinates(estaciones)
    # Agregamos las coordenadas x e y (GK) de cada estación.
    estaciones <- data.frame(sp::coordinates(GK.coords), estaciones)

    # Obtenemos los registros diarios de las estaciones con las que vamos a trabajar.
    omm_ids <- paste(estacionesID, collapse = ',')

    rs.registros.diarios <- RPostgreSQL::dbSendQuery(conexion, sprintf(queries$daily_records, omm_ids, omm_ids))
    registrosDiarios <- RPostgreSQL::fetch(rs.registros.diarios,n=-1, stringsAsFactors=FALSE)

    # Convertimos las fechas de la query en variables Date.
    registrosDiarios$fecha <- as.Date(registrosDiarios$fecha)

    missing.rad <- RPostgreSQL::fetch(RPostgreSQL::dbSendQuery(conexion, sprintf(queries$missing_radiation_count, omm_ids)), n=-1, stringsAsFactors=FALSE)

    # Check invalid temperatures to avoid DSSAT errors.
    wrong_temps <- which(registrosDiarios$tmax <= registrosDiarios$tmin)
    # Make invalid temperatures NA so the impute methods recalculate them.
    registrosDiarios[wrong_temps, c('tmax', 'tmin')] <- NA

    for(estacion in estacionesID) {
        # Creamos una transacción por cada estación.
        response <- RPostgreSQL::dbGetQuery(conexion, "BEGIN TRANSACTION")

        datosEstacion <- registrosDiarios[registrosDiarios$omm_id == estacion,]
        indexesToWrite <- c()

        # Obtenemos los ID's y las coordenadas de cada estación vecina.
        vecinos.data <- RPostgreSQL::fetch(RPostgreSQL::dbSendQuery(conexion, sprintf(queries$neighbors_data, estacion)), n=-1, stringsAsFactors=FALSE)

        # Calculamos las coordenadas en GK.
        GK.coords <- Gauss.Kruger.coordinates(vecinos.data)

        # Agregamos las coordenadas x e y al data frame de vecinos.
        vecinos.data <- data.frame(sp::coordinates(GK.coords), omm_id=GK.coords@data$omm_id)

        # Traer registros de vecinos.
        vecinos.query <- sprintf(queries$neighbors_records, paste(vecinos.data$omm_id, collapse = ','))

        registrosVecinos <- NULL

        for (variable in c('tmax', 'tmin', 'prcp')) {
            missingIndexes <- which(is.na(datosEstacion[, variable]))

            writeLines(paste0("> Station: ", estacion, ". Variable: ", variable, ". Missing: ", length(missingIndexes)))

            # Check if there are missing values for this station and variable, otherwise, skip it.
            if(length(missingIndexes) == 0) next;

            indexesToWrite <- c(indexesToWrite, missingIndexes)

            # Query the DB for neighbor's data. Done only if there are missing values.
            if(is.null(registrosVecinos)) {
                registrosVecinos <- RPostgreSQL::fetch(RPostgreSQL::dbSendQuery(conexion, vecinos.query), n=-1, stringsAsFactors=FALSE)
                registrosVecinos$fecha <- as.Date(registrosVecinos$fecha)
                registrosVecinos <- dplyr::as.tbl(registrosVecinos) %>% dplyr::left_join(vecinos.data, by='omm_id')
            }

            if (variable == 'prcp') {
                datosEstacion <- impute_mf(datosEstacion, variable, estaciones, missingIndexes, registrosVecinos, vecinos.data, opt$parallelism)
            } else {
                datosEstacion <- impute_idw(datosEstacion, variable, estaciones, missingIndexes, registrosVecinos, vecinos.data, opt$parallelism)
            }
        }

        if(length(indexesToWrite) > 0) {
            indexesToWrite <- unique(indexesToWrite)

            # Print progress.
            writeLines(paste0("> Station: ", estacion, ". Imputed: ", length(indexesToWrite)))

            queryRowFormat <- "(%d, '%s', %.2f, %.2f, NULL, NULL, NULL, NULL, %.2f, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)"

            inserts <- split(indexesToWrite, ceiling(seq_along(indexesToWrite)/20))

            for(paquete in inserts) {
                # Insertamos tmax, tmin y prcp.
                values <- apply(datosEstacion[paquete, ], 1, function(r) {
                    sprintf(queryRowFormat, as.integer(r['omm_id']), r['fecha'], as.numeric(r['tmax']), as.numeric(r['tmin']), as.numeric(r['prcp']))
                })
                values <- paste0(values, collapse = ',')
                insert_query <- paste0(queries$insert_imputed, values)
                insert_and_check(conexion, insert_query, expected_results = length(paquete))
            }
        }

        missing.rad.count <- missing.rad[missing.rad$omm_id == estacion, "count"]

        if(is.null(missing.rad.count) | anyNA(missing.rad.count)) next;

        if(length(missing.rad.count) > 0) {
            # Update radiation only.
            missing.rad.dates.query <- sprintf(queries$missing_rad_dates, estacion)
            missing_dates <- as.Date(RPostgreSQL::fetch(RPostgreSQL::dbSendQuery(conexion, missing.rad.dates.query), n=-1, stringsAsFactors=FALSE)$fecha)

            codigo_pais <- toupper(estaciones[estaciones$omm_id == estacion, 'pais_id'])

            if(!codigo_pais %in% names(srad_parameters)) codigo_pais <- 'AR'

            records <- datosEstacion %>% filter(fecha %in% missing_dates)

            estimado <- estimarRadiacion(estaciones=estaciones[estaciones$omm_id == estacion, ],
                                         registrosDiarios=records,
                                         ap.cal = srad_parameters[[codigo_pais]]$ap,
                                         bc.cal = srad_parameters[[codigo_pais]]$bc,
                                         svk.cal = srad_parameters[[codigo_pais]]$svk)

            if(estimado$not_estimated > 0) {
                error_details <- paste0("Failed to estimate ", estimado$not_estimated, " radiation values for station ", estacion, ". Rolling back it's data.")
                errors <<- c(errors, error_details)
                writeLines(error_details)
                RPostgreSQL::dbRollback(conexion)
                next;
            } else {
                estimado <- estimado$results %>% dplyr::select(dplyr::one_of('omm_id', 'fecha', 'rad'))

                queryRadFormat <- "(%d, '%s', %.2f)"

                writeLines(paste0("> Station: ", estacion, ". Radiation: ", nrow(estimado)))

                inserts <- split(seq(from=1, to=nrow(estimado)), ceiling(seq(from=1, to=nrow(estimado))/50))

                # Insertamos radiación.
                for(paquete in inserts) {
                    values <- apply(estimado[paquete, ], 1, function(r) {
                        sprintf(queryRadFormat, as.integer(r['omm_id']), r['fecha'], as.numeric(r['rad']))
                    })
                    insert_query <- paste0(queries$insert_radiation, paste0(values, collapse = ','))
                    insert_and_check(conexion, insert_query, expected_results = length(paquete))
                }
            }
        }

        RPostgreSQL::dbCommit(conexion)
    }
}, warning = function(warn){
    errors <<- c(errors, warn)
}, error = function(error_detail) {
    errors <<- c(errors, error_detail)
}, finally =  {
    if (length(errors) > 0 | exit_status != 0) {
        exit_status <<- 1
        writeLines('> Errors arised, see log for details.')
        writeLines(paste(errors, collapse = '\n\n'))
    } else {
        writeLines('> Success.')
    }
    RPostgreSQL::dbDisconnect(conexion)
})

quit(status=exit_status)
