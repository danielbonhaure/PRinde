### Estima la radiación solar diaria en MJ/m² tomando como parámetro las mediciones de las
### variables requeridas por el método.
### Recibe además el objeto devuelto por el método de calibración y, opcionalmente,
### el argumento del factor.
estimate.hargreaves <-  function(tmax, tmin, extrat, ha.coef, factor.arg=NULL)  {
    if(is.null(factor.arg)){
        # Si no se pasa el factor es una calibración única.
        return(extrat * ha.coef[['A']] * sqrt(tmax - tmin) + ha.coef[['B']])
    } else {
        df <- data.frame(tmax, tmin, extrat, factor.arg)
        # Por cada fila del data frame llamamos a esta misma función con los parámetros
        # de calibración que corresponden a esa fila en base al argumento del factor.
        res <- apply(df, 1, function(x) estimate.hargreaves(x['tmax'], x['tmin'], x['extrat'], ha.coef[x['factor.arg'],]))
        return(res)
    }
}

### Calibra el método de Hargreaves utilizando, opcionalmente, un facor para agrupar los datos.
calibrate.hargreaves <- function(tmax, tmin, extrat, solar.rad, factor.arg=NULL) {    
    if(!is.null(factor.arg)){
        fact <- factor(factor.arg)
        
        # Calculamos el valor del método de Hargreaves para cada registro.
        ha.value <- extrat*sqrt(tmax-tmin)
        # Realizamos el ajuste del valor con la interacción del factor.
        fit <- lm(solar.rad ~ ha.value + fact + ha.value*fact)
        
        coeff <- coef(fit)
        
        initA <- coeff[2]
        initB <- coeff[1]
        lev <- levels(fact)
        
        # Armamos el data frame que vamos a devolver con los niveles del factor
        # y el valor de 'A' y 'B' para cada nivel.
        cal <- data.frame(level=lev, A=c(initA), B=c(initB), row.names=NULL)
        
        # Al valor inicial que nos da la calibración le sumamos el valor
        # del factor para cada nivel.
        for(i in 2:length(lev)){
            cal[i,]$A <- cal[i,]$A + coeff[length(coeff) / 2 + i]
            cal[i,]$B <- cal[i,]$B + coeff[i+1]
        }
        
        return(cal)
    } else {
        # Si no se para un factor para usar de argumento, realizamos una simple
        # regresión lineal y devolvemos la pendiente (A) y la intersección (B).
        fit <- lm(solar.rad ~ I(extrat*sqrt(tmax-tmin)))
        coeff <- coef(fit)
        names(coeff) <- c('B', 'A')
        return(coeff)
    }
}