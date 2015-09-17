### Estima la radiación solar diaria en MJ/m² tomando como parámetro las mediciones de las
### variables requeridas por el método.
### Recibe además el objeto devuelto por el método de calibración y, opcionalmente,
### el argumento del factor.
estimate.angstromprescott <- function(rel.helio, extrat, ap.coef, factor.arg=NULL) {
    if(is.null(factor.arg)){
        # Si no se pasa el factor es una calibración única.
        return(extrat * (rel.helio * ap.coef[['A']] + ap.coef[['B']]))
    } else {
        df <- data.frame(rel.helio, extrat, factor.arg)
        # Por cada fila del data frame llamamos a esta misma función con los parámetros
        # de calibración que corresponden a esa fila en base al argumento del factor.
        res <- apply(df, 1, function(x) estimate.angstromprescott(x['rel.helio'], x['extrat'], ap.coef[x['factor.arg'],]))
        return(res)
    }
}

### Calibra el método de Angström-Prescott utilizando, opcionalmente, un facor para agrupar los datos.
calibrate.angstromprescott <- function(rel.helio, extrat, solar.rad, factor.arg=NULL) {
    # Calculamos la radiación solar relativa.
    rel.solrad <- solar.rad / extrat
    
    if(!is.null(factor.arg)){
        # Si nos pasan un factor, calibramos el modelo con el valor
        # de la heliofnaía relativa y la interacción con dicho factor.
        fact <- factor(factor.arg)
        
        fit <- lm(rel.solrad ~ rel.helio + fact + rel.helio*fact)
        
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
        fit <- lm(rel.solrad ~ rel.helio)
        coeff <- coef(fit)
              
        names(coeff) <- c('B', 'A')
        return(coeff)
    }
}