source('methods/Angstrom-Prescott.R')
source('methods/Bristow-Campbell.R')
source('methods/Supit-Van Kappel.R')


ap.cal.default <- c(A=0.58, B=0.2)
bc.cal.default <- c(A=0.69, B=0.02, C=2.12)
svk.cal.default <- c(A=0.06, B=0.47, C=0.8)

estimarRadiacion <- function(estaciones, registrosDiarios, ap.cal=NULL, bc.cal=NULL, svk.cal=NULL) {
    if(is.null(ap.cal)) ap.cal <- ap.cal.default;
    if(is.null(bc.cal)) bc.cal <- bc.cal.default;
    if(is.null(svk.cal)) svk.cal <- svk.cal.default;

    totalNoEstimado <- 0

    ## Llenamos la radiación extraterrestre y la duración del día en horas para cada registro.
    for(i in 1:nrow(estaciones)){
        estacion <- estaciones[i, ]

        # Obtenemos los índices de los registros que corresponden a la estación en cuestión.
        registros_idx <- which(registrosDiarios$omm_id == estacion$omm_id)

        # Calculamos la radiación extraterrestre y el largo del día en horas para cada día.
        extrat.data <- extrat(i=dayOfYear(registrosDiarios[registros_idx, "fecha"]), lat=radians(estacion$lat_dec))

        registrosDiarios[registros_idx, 'extrat'] <- extrat.data$ExtraTerrestrialSolarRadiationDaily
        registrosDiarios[registros_idx, 'daylength'] <- extrat.data$DayLength

        registros_estacion <- registrosDiarios[registros_idx, ]
        # Determinamos los índices de los registros que se pueden calcular por cada método.
        bcIndexes <- which(x=(!is.na(registrosDiarios[registros_idx, "tmax"]) & !is.na(registrosDiarios[registros_idx, "tmin"])))
        bcIndexes <- registros_idx[bcIndexes]

        svkIndexes <- which(x=(!is.na(registrosDiarios[bcIndexes, "nub"])))
        svkIndexes <- bcIndexes[svkIndexes]

        apIndexes <- which(x=(!is.na(registrosDiarios[registros_idx, "helio"])))
        apIndexes <- registros_idx[apIndexes]

        # Filtramos los índices para calcular con los métodos de más precisión (AP > SVK > BC).
        svkIndexes <- svkIndexes[!svkIndexes %in% apIndexes]
        bcIndexes <- bcIndexes[!bcIndexes %in% apIndexes & !bcIndexes %in% svkIndexes]

        if(length(apIndexes) > 0) {
            relHelio <- (registrosDiarios[apIndexes,"helio"] / registrosDiarios[apIndexes,"daylength"])

            registrosDiarios[apIndexes,"rad"] <- estimate.angstromprescott(rel.helio= relHelio,
                                                                           extrat= registrosDiarios[apIndexes,"extrat"],
                                                                           ap.coef= ap.cal)
        }

        if(length(svkIndexes) > 0){
            registrosDiarios[svkIndexes,"rad"] <- estimate.supitvankappel(tmax= registrosDiarios[svkIndexes,"tmax"],
                                                                          tmin= registrosDiarios[svkIndexes,"tmin"],
                                                                          cloudcover= registrosDiarios[svkIndexes,"nub"],
                                                                          extrat= registrosDiarios[svkIndexes,"extrat"],
                                                                          svk.coef= svk.cal)
        }

        if(length(bcIndexes) > 0) {
            bcEstimate <- estimate.bristowcampbell.xts(xtsdata=xts(registrosDiarios[registros_idx,], order.by=registrosDiarios[registros_idx, "fecha"]),
                                                       days= registrosDiarios[bcIndexes, "fecha"],
                                                       bc.coef= bc.cal)

            registrosDiarios[bcIndexes, "rad"] <- bcEstimate
        }

        noEstimados <- length( which(is.na(registrosDiarios[registros_idx,]$rad)))

        totalNoEstimado <- totalNoEstimado + noEstimados

        # Imprimimos un resumen de la estimación realizada.
#         writeLines(paste0("Estación ", estacion$omm_id, " (", estacion$nombre, ")", "\n\t ",
#                           length(bcIndexes), " registros se estiman por BC", "\n\t\t",
#                           length(svkIndexes), " por SVK\n\t\t",
#                           length(apIndexes), " por AP\n\t\t",
#                           "Registros totales: ", length(registros), " (",
#                           (noEstimados), " no estimados).\n"))
    }

#     writeLines(paste("Total de registros procesados: ", nrow(registrosDiarios)))
#     writeLines(paste("Total de registros no estimados: ", totalNoEstimado))

    l <- list()
    l$not_estimated <- totalNoEstimado
    l$results <- registrosDiarios
    return(l)
}
