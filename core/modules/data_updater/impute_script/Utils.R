
Gauss.Kruger.coordinates <- function(df) {
    sp::spTransform(sp::SpatialPointsDataFrame(cbind(x = df[,"lon_dec"], y = df[,"lat_dec"]), data=df,
                                       proj4string = sp::CRS("+proj=longlat +datum=WGS84 +no_defs +ellps=WGS84 +towgs84=0,0,0"), bbox = NULL),
                sp::CRS("+proj=tmerc +lat_0=-90 +lon_0=-60 +k=1 +x_0=5500000 +y_0=0 +ellps=intl +twogs84=-148,136,90,0,0,0,0 +units=m +no_defs"))
}

insert_in_packages <- function(db_connection, packet_size=50) {

}

insert_and_check <- function(db_connection, insert_query, expected_results) {
    rs <- RPostgreSQL::dbSendQuery(db_connection, insert_query)
    rowsAffected <- RPostgreSQL::dbGetInfo(rs, what = "rowsAffected")

    if(rowsAffected != expected_results) {
        RPostgreSQL::dbRollback(conexion)
        return(paste0("Mismatch between affected rows and insert packet length (",
                      rowsAffected, " != ", expected_results, ")."))
    }
}
