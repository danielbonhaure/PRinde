
queries <- list()

# queries$daily_records <- paste("SELECT DISTINCT ON (omm_id, fecha) fecha, omm_id, tmax, tmin, prcp, helio, nub FROM (",
#                                "SELECT 1 as priority, omm_id, fecha, tmax, tmin, prcp, helio, nub FROM estacion_registro_diario_completo erdc WHERE erdc.omm_id IN (%s)",
#                                "UNION",
#                                "SELECT 2 as priority, omm_id, fecha, tmax, tmin, prcp, helio, nub FROM estacion_registro_diario erd WHERE erd.omm_id IN (%s)",
#                                ") AS subQuery",
#                                "ORDER BY omm_id, fecha, priority")
queries$daily_records <- paste("SELECT erd.omm_id, erd.fecha, coalesce(erdi.tmax, erd.tmax) tmax, coalesce(erdi.tmin, erd.tmin) tmin, coalesce(erdi.prcp, erd.prcp) prcp, rad, erd.helio, erd.nub",
                               "FROM estacion_registro_diario erd",
                               "FULL OUTER JOIN estacion_registro_diario_imputado erdi ON erdi.omm_id = erd.omm_id AND erdi.fecha = erd.fecha",
                               "LEFT JOIN estacion_radiacion_diaria rad ON erd.omm_id = rad.omm_id AND erd.fecha = rad.fecha",
                               "WHERE erd.omm_id IN (%s)",
                               "ORDER BY omm_id, erd.fecha")

queries$missing_radiation_count <- paste("SELECT erd.omm_id, COUNT(erd.fecha) FROM estacion_registro_diario erd",
                                         "LEFT JOIN estacion_radiacion_diaria erad ON erd.omm_id = erad.omm_id AND erd.fecha = erad.fecha",
                                         "WHERE erd.omm_id = ANY(array[%s]) AND erad.fecha ISNULL",
                                         "GROUP BY erd.omm_id")

queries$neighbors_data <- paste("SELECT e.omm_id, e.lat_dec, e.lon_dec FROM estacion_vecino ev",
                                "LEFT JOIN estacion e ON e.omm_id = ev.omm_id",
                                "WHERE ev.omm_vecino_id = %s",
                                "ORDER BY distancia, diferencia_elevacion ASC LIMIT 7")

queries$neighbors_records <- "SELECT erd.omm_id, fecha, tmax, tmin, prcp FROM estacion_registro_diario erd WHERE erd.omm_id IN (%s) ORDER BY 2"

queries$insert_imputed <- paste0("INSERT INTO estacion_registro_diario_imputado(omm_id, fecha, tmax, tmin, tmed, td, pres_est, pres_nm, prcp, ",
                                 "hr, helio, nub, vmax_d, vmax_f, vmed, estado, num_observaciones) VALUES ")

queries$insert_radiation <- "INSERT INTO estacion_radiacion_diaria(omm_id, fecha, rad) VALUES "

queries$missing_rad_dates <- paste("SELECT erd.fecha FROM estacion_registro_diario erd",
                                   "LEFT JOIN estacion_radiacion_diaria erad ON erd.omm_id = erad.omm_id AND erd.fecha = erad.fecha",
                                   "WHERE erd.omm_id = %s AND erad.fecha ISNULL")