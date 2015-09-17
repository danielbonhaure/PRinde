### Estima la radiación solar diaria en MJ/m² tomando como parámetro las mediciones de las
### variables requeridas por el método.
### Recibe además el objeto devuelto por el método de calibración y, opcionalmente,
### el argumento del factor.
estimate.supitvankappel <-  function(tmax, tmin, cloudcover, extrat, svk.coef, factor.arg=NULL)  {
    if(is.null(factor.arg)){
        A <- svk.coef[['A']]
        B <- svk.coef[['B']]
        C <- svk.coef[['C']]
        
        # Si no se pasa el factor es una calibración única.
        return(extrat * (A*sqrt(tmax - tmin) + B*sqrt(1 - cloudcover/8)) + C)
    } else {
        df <- data.frame(tmax, tmin, cloudcover, extrat, factor.arg)
        # Por cada fila del data frame llamamos a esta misma función con los parámetros
        # de calibración que corresponden a esa fila en base al argumento del factor.
        res <- apply(df, 1, function(x) estimate.supitvankappel(x['tmax'], x['tmin'], x['cloudcover'], x['extrat'], svk.coef[x['factor.arg'],]))
        return(res)
    }
}

### Calibra el método de Supit-Van Kappel utilizando, opcionalmente, un facor para agrupar los datos.
calibrate.supitvankappel <- function(tmax, tmin, cloudcover, extrat, solar.rad, factor.arg=NULL) {    
    if(!is.null(factor.arg)){
        fact <- factor(factor.arg)
        
        # Calculamos el valor del método de Hargreaves para cada registro.
        ha.value <- extrat * sqrt(tmax-tmin)
        # Calculamos el agregado de Supit.
        su.value <- extrat * sqrt(1 - cloudcover / 8)
        # Realizamos el ajuste del valor con la interacción del factor.
        fit <- lm(solar.rad ~ su.value + ha.value + su.value*fact + ha.value*fact + fact)
        
        coeff <- coef(fit)
        
        initA <- coeff[3]
        initB <- coeff[2]
        initC <- coeff[1]
        lev <- levels(fact)
        
        # Armamos el data frame que vamos a devolver con los niveles del factor
        # y el valor de 'A', 'B' y 'C' para cada nivel.
        cal <- data.frame(level=lev, A=c(initA), B=c(initB), C=c(initC), row.names=NULL)
        
        # Al valor inicial que nos da la calibración le sumamos el valor
        # del factor para cada nivel.
        for(i in 2:length(lev)){
            cal[i,]$A <- cal[i,]$A + coeff[length(coeff) * (2/3) + i]
            cal[i,]$B <- cal[i,]$B + coeff[length(coeff) / 3 + 1 + i]
            cal[i,]$C <- cal[i,]$C + coeff[i+2]
        }
        
        return(cal)
    } else {
        # Si no se para un factor para usar de argumento, realizamos una simple
        # regresión lineal y devolvemos la pendiente de cada término (A y B) y
        # la intersección (C).
        fit <- lm(solar.rad ~ I(extrat * sqrt(1 - cloudcover/8)) + I(extrat * sqrt(tmax-tmin)))
        coeff <- coef(fit)
        names(coeff) <- c('C' ,'B', 'A')
        return(coeff)
    }
}