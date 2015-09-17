### Estima la radiación solar diaria en MJ/m² tomando como parámetro las mediciones de las
### variables requeridas por el método.
estimate.almorox <-  function(tmax, tmin, extrat, precip, rel.humidity, almx.coef) {
    if(is.null(almx.coef)) {
        ret <- rep(x=NA, times=length(extrat))
        return(ret)
    }
    
    dfdata <- .df_almorox(tmax, tmin, extrat, precip, rel.humidity)
    
    A <- almx.coef[['A']]
    B <- almx.coef[['B']]
    C <- almx.coef[['C']]
    D <- almx.coef[['D']]
    E <- almx.coef[['E']]
    
    # Estimamos la radiación diaria en MJ/m²/día.
    almx <- A * dfdata$extrat * (1 - exp(B * ((dfdata$es.tmin/dfdata$es.tmax)^C))) * (1 + D * dfdata$rt + E * dfdata$rh)
    return(almx)
}

### Realiza la calibración de Donatelli-Campbell a partir de 4 ó 5 vectores con las variables
### meteorológicas que dicho método requiere.
calibrate.almorox <- function(tmax, tmin, extrat, precip, rel.humidity, solar.rad) {
    dfdata <- .df_almorox(tmax, tmin, extrat, precip, rel.humidity, solar.rad)
    
    dfdata <- na.omit(dfdata)
    
    Alm <- NULL
    
    # Calibramos el modelo.
    try(Alm <- nls(solar.rad ~ A * extrat * (1 - exp(B * ((es.tmin/es.tmax)^C))) * (1 + D*rt + E*rh), 
                 dfdata, start=list(A=0.73, B=-0.25, C=-2.65, D=-0.12, E=-0.0043),  control=list(maxiter = 500)), silent=TRUE)
    
    if(is.null(Alm)) {
        warning('Couldn\'t fit Almorox model to dataset (singular gradient).')
        return(NULL);
    }
    
    return(coef(Alm))
}


### Acomoda los campos que necesita la función de BC en un data frame.
### Calcula además la diferencia de temperatura y la agrega como una columna.
.df_almorox <- function(tmax, tmin, extrat, precip, rel.humidity, solar.rad = NULL) {    
    # Armamos un data frame para tener alineados en una misma fila todas las variables.
    data <- data.frame(extrat, rh=rel.humidity)
    
    if(!is.null(solar.rad))
        data <- cbind(data, solar.rad)
    
    es.tmin <- sat.vapor.pressure(tmin)
    es.tmax <- sat.vapor.pressure(tmax)
    
    rt <- ifelse(precip > 0, 1, 0)

    
    return(cbind(data, es.tmin, es.tmax, rt))
}

sat.vapor.pressure <- function(temp) {
    return( 0.6108*exp((17.27*temp)/(temp + 237.3)) )
}