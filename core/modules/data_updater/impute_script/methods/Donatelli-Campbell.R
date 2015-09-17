### Estima la radiación solar diaria en MJ/m² tomando como parámetro las mediciones de las
### variables requeridas por el método.
### Si no se nos pasa la temperatura mínima del día siguiente se calculará asumiendo que la
### secuencia de observaciones de T. Mín. es consecutiva, cometiendo ciero grado de error en
### caso de no ser así.
estimate.donatellicampbell <- function(tmax, tmin, extrat, dc.coef, tmin.next = NULL) {
    if(is.null(dc.coef)) {
        ret <- rep(x=NA, times=length(extrat))
        return(ret)
    }
    
    data <- .df_donatellicampbell(tmax, tmin, tmin.next, extrat)
    
    # Estimamos la radiación diaria en MJ/m²/día.
    dc <-  data$extrat * dc.coef[['A']] * (1 - exp(-dc.coef[['B']] * data$f.tavg * (data$dTemp^2) * exp( data$tmin / dc.coef[['C']] )))
    return(dc)
}

### Estima la radiación solar diaria en MJ/m² tomando como parámetro una serie XTS con los campos
### requeridos por el método.
### Además, permite especificar qué días de dicha serie utilizar para estimar.
estimate.donatellicampbell.xts <- function(xtsdata, dc.coef, days = NULL, fieldnames=c('tmax', 'tmin', 'extrat')) {
    data <- .xts_donatellicampbell(xtsdata, days, fieldnames)
    
    # Extraemos los campos de la serie XTS.
    Tmax <- data[, fieldnames[1]]
    Tmin <- data[, fieldnames[2]]
    extraT <- data[, fieldnames[3]]
    tmin.next <- data[, 'tmin.next']
    
    return(estimate.donatellicampbell(Tmax, Tmin, extraT, dc.coef, tmin.next))
}


### Realiza la calibración de Donatelli-Campbell a partir de 4 ó 5 vectores con las variables
### meteorológicas que dicho método requiere.
### Si no se nos pasa la temperatura mínima del día siguiente se calculará asumiendo que la
### secuencia de observaciones de T. Mín. es consecutiva, cometiendo ciero grado de error en
### caso de no ser así.
calibrate.donatellicampbell <- function(tmax, tmin, extrat, solar.rad, tmin.next = NULL) {
    dfdata <- .df_donatellicampbell(tmax, tmin, tmin.next, extrat, solar.rad)
    
    dfdata <- na.omit(dfdata)
    
    DCCal <- NULL
    
    # Calibramos el modelo.
    try(DCCal <- robustbase::nlrob((solar.rad/extrat) ~ A * (1 - exp(-B * f.tavg * (dTemp^2) * exp( tmin / C ))), 
                 dfdata, start=list(A=0.7, B=1, C=50),  control=list(maxiter = 500, minFactor=0.00001)), silent=FALSE)

    if(is.null(DCCal)) {
        warning('Couldn\'t fit Donatelli-Campbell model to dataset (singular gradient).')
        return(NULL);  
    }
    return(coef(DCCal))
}


### Realiza la calibración del método de Donatelli-Campbell a partir de un objeto XTS.
### Recibe además los días de dicha secuencia que se deben usar para calibrar y los nombres
### de las columnas que corresponden a cada variable requerida.
### Tiene la ventaja de que calcula la temperatura mínima del día siguiente de manera certera.
calibrate.donatellicampbell.xts <- function(xtsdata, days = NULL, fieldnames=c('tmax', 'tmin', 'extrat', 'solar.rad')) {
    data <- .xts_donatellicampbell(xtsdata, days, fieldnames)
    
    # Extraemos los campos de la serie XTS.
    tmax <- data[, fieldnames[1]]
    tmin <- data[, fieldnames[2]]
    extrat <- data[, fieldnames[3]]
    rad <- data[, fieldnames[4]]
    tmin.next <- data[, 'tmin.next']
    
    return(calibrate.donatellicampbell(tmax, tmin, extrat, rad, tmin.next))
}


### Extrae los datos necesarios de la serie XTS y los devuelve como un dataframe.
### 'Laggea' la serie XTS para calcular la T. Mínima del día siguiente.
.xts_donatellicampbell <- function(xtsdata, days, fieldnames) {
    # Si no nos pasan una lista de días, tomamos todos los de la serie XTS.
    if(is.null(days))
        days <- index(xtsdata)
    
    # Obtenemos la temperatura mínima del día siguiente.
    TminNext <- lag.xts(xtsdata, k=-1)[, fieldnames[2]]
    colnames(TminNext) <- c('tmin.next')
    data <- cbind.xts(xtsdata, TminNext)
    
    # Reemplazamos los valores NA de temperatura del día siguiente con el valor de la medición
    # del mismo día.
    data$tmin.next[is.na(data$tmin.next)] <- data[is.na(data$tmin.next), fieldnames[2]]
    
    # Filtramos los días que hay que usar para calibrar.
    data <- data[index(data) %in% days,]
    # Convertimos los datos de la serie a un data frame.
    data <- data.frame(coredata(data[, c(fieldnames, 'tmin.next')]))
    
    return(data)
}


### Acomoda los campos que necesita la función de BC en un data frame.
### Calcula además la diferencia de temperatura y la agrega como una columna.
.df_donatellicampbell <- function(tmax, tmin, tmin.next, extrat, solar.rad = NULL) {
    # Si no nos pasan el vector de temperatura mínima del día siguiente, lo construimos
    # asumiendo que no hay días faltantes.
    if(is.null(tmin.next)) 
        tmin.next <- c(tmin[-1], tmin[length(tmin)])
    
    # Armamos un data frame para tener alineados en una misma fila todas las variables.
    data <- data.frame(tmax, tmin, tmin.next, extrat)
    
    if(!is.null(solar.rad))
        data <- cbind(data, solar.rad)
    
    # Calculamos la diferencia de temperatura de cada día.
    dTemp <- data$tmax - (data$tmin + data$tmin.next) / 2
    
    tavg <- (data$tmax + data$tmin) / 2
    f.tavg <- 0.017*exp(exp(-0.053*tavg)) 
    
    return(cbind(data, dTemp, tavg, f.tavg))
}