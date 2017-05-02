CREATE EXTENSION IF NOT EXISTS cube;
CREATE EXTENSION IF NOT EXISTS earthdistance;
DROP FUNCTION IF EXISTS pr_create_campaigns(int, varchar, varchar, varchar, varchar);
DROP FUNCTION IF EXISTS pr_crear_serie(int, date, date, date, int);
DROP FUNCTION IF EXISTS pr_año_agrario(date);
DROP FUNCTION IF EXISTS pr_año_agrario(date, int);
DROP FUNCTION IF EXISTS is_leap(int);
DROP FUNCTION IF EXISTS last_day(date);
DROP FUNCTION IF EXISTS pr_campañas_completas(int);
DROP FUNCTION IF EXISTS pr_campañas_completas(int, int);
DROP FUNCTION IF EXISTS pr_campaigns_acum_rainfall(int);
DROP FUNCTION IF EXISTS pr_campaigns_acum_rainfall(int, int);
DROP FUNCTION IF EXISTS pr_campaigns_rainfall(int);
DROP FUNCTION IF EXISTS pr_campaigns_rainfall(int, int);
DROP FUNCTION IF EXISTS pr_historic_series(int, varchar);
DROP FUNCTION IF EXISTS pr_serie_agraria(int, int);

/* Función que determina si un año es bisiesto. */
CREATE OR REPLACE FUNCTION is_leap(year integer)
RETURNS BOOLEAN AS $$
	SELECT ($1 % 4 = 0) AND (($1 % 100 <> 0) OR ($1 % 400 = 0))
$$ LANGUAGE sql;


CREATE OR REPLACE FUNCTION last_day(date)
RETURNS date AS $$
    SELECT (date_trunc('MONTH', $1) + '1 MONTH - 1 day'::interval)::date;
$$ LANGUAGE sql;

CREATE OR REPLACE FUNCTION pr_año_agrario(fecha date, mes_fin_de_campaña int default 5)
RETURNS DOUBLE PRECISION AS $$
    SELECT CASE WHEN(EXTRACT(MONTH FROM $1) < mes_fin_de_campaña)
                      THEN EXTRACT(YEAR FROM $1)-1
                ELSE EXTRACT(YEAR FROM $1)
           END
$$ LANGUAGE sql;


CREATE OR REPLACE FUNCTION pr_campañas_completas(omm_id int, mes_fin_de_campaña int default 5)
RETURNS TABLE (pr_year INT) AS $$
    WITH count_agrario AS (
        SELECT DISTINCT pr_año_agrario(fecha, mes_fin_de_campaña)::int AS pr_year, COUNT(1) AS count
            FROM estacion_registro_diario erd
            WHERE erd.omm_id = $1
            GROUP BY pr_year
    )
    SELECT c2.pr_year FROM count_agrario c2
    LEFT JOIN count_agrario c1 ON c2.pr_year-1 = c1.pr_year
    LEFT JOIN count_agrario c3 ON c2.pr_year+1 = c3.pr_year
    WHERE c2.count >= 365 AND c1.count >= 60 AND c3.count >= 60
    ORDER BY 1
$$ LANGUAGE sql;


CREATE OR REPLACE FUNCTION pr_campaigns_acum_rainfall(omm_id INT, mes_fin_de_campaña int default 5)
RETURNS TABLE(fecha DATE, campaign INT, sum DOUBLE PRECISION)
AS $$
    SELECT erd.fecha,
        pr_año_agrario(erd.fecha, mes_fin_de_campaña)::int,
        SUM(erd.prcp) OVER (PARTITION BY pr_año_agrario(erd.fecha, mes_fin_de_campaña) ORDER BY erd.fecha)
    FROM estacion_registro_diario_completo erd
    WHERE erd.omm_id IN ($1) AND pr_año_agrario(erd.fecha) IN (SELECT pr_campañas_completas($1, $2))
$$ LANGUAGE SQL;


CREATE OR REPLACE FUNCTION pr_campaigns_rainfall(omm_id INT, mes_fin_de_campaña int default 5)
RETURNS TABLE(fecha DATE, campaign INT, sum DOUBLE PRECISION)
AS $$
    SELECT erd.fecha, pr_año_agrario(erd.fecha, mes_fin_de_campaña)::int, erd.prcp
    FROM estacion_registro_diario_completo erd
    WHERE erd.omm_id IN ($1) AND pr_año_agrario(erd.fecha, mes_fin_de_campaña) IN (SELECT pr_campañas_completas($1, $2))
$$ LANGUAGE SQL;


CREATE OR REPLACE FUNCTION pr_crear_serie(omm_id int, fecha_inicio date, fecha_inflexion date, fecha_fin date, year_inflexion int)
RETURNS TABLE (fecha date, fecha_original date, tmax double precision, tmin double precision, prcp double precision, rad double precision)
AS $$
    DECLARE
        id_estacion ALIAS FOR $1;
        fecha_inicio_inflexion DATE;
        fecha_fin_inflexion DATE;
        monthday VARCHAR;
    BEGIN
        -- Extraemos el mes y el día juntos, con el formato especificado.
        monthday := TO_CHAR(fecha_inflexion, '-MM-DD');

        -- Creamos la fecha de inicio de los datos climáticos históricos.
        IF ( monthday = '-02-29' ) THEN
            fecha_inicio_inflexion := (year_inflexion || '-03-01')::date;
        ELSE
            fecha_inicio_inflexion := (year_inflexion || monthday)::date + '1 day'::interval;
        END IF;
    
        -- Creamos la fecha de fin de los datos climáticos históricos.
        fecha_fin_inflexion := fecha_inicio_inflexion + (fecha_fin - fecha_inflexion) - '1 day'::interval;

        RETURN QUERY
        WITH datos_raw AS (
            SELECT erd.fecha AS fecha_original, erd.tmax, erd.tmin, erd.prcp, erd.rad, 1 AS orden
            FROM estacion_registro_diario_completo erd
            WHERE erd.omm_id = id_estacion AND (erd.fecha BETWEEN fecha_inicio AND fecha_inflexion)
            UNION
            SELECT erd.fecha AS fecha_original, erd.tmax, erd.tmin, erd.prcp, erd.rad, 2 AS orden
            FROM estacion_registro_diario_completo erd
            WHERE erd.omm_id = id_estacion AND (erd.fecha BETWEEN fecha_inicio_inflexion AND fecha_fin_inflexion)
        ), datos_ordenados AS (
            SELECT row_number() OVER (ORDER BY dc.orden, dc.fecha_original ASC) AS row_number, dc.fecha_original,
                   dc.tmax, dc.tmin, dc.prcp, dc.rad
            FROM datos_raw dc
        ), fechas_ordenadas AS (
            SELECT row_number() OVER (ORDER BY fechas_generadas ASC) AS row_number, fechas_generadas::date
            FROM generate_series( fecha_inicio, fecha_fin, '1 day'::interval) fechas_generadas
        )
        SELECT fo.fechas_generadas, datos.fecha_original, datos.tmax, datos.tmin, datos.prcp, datos.rad
        FROM datos_ordenados datos INNER JOIN fechas_ordenadas fo ON fo.row_number = datos.row_number;
    END
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION pr_serie_agraria(omm_id int, year_agrario int, mes_fin_de_campaña int default 5)
RETURNS TABLE (fecha date, fecha_original date, tmax double precision, tmin double precision, prcp double precision, rad double precision)
AS $$
    DECLARE
        id_estacion ALIAS FOR $1;
        año_agrario ALIAS FOR $2;

        fecha_inicio DATE;
        fecha_fin DATE;
        fecha_inicio_serie DATE;
        fecha_fin_serie DATE;
    BEGIN
        fecha_inicio := (format('%s-%s-01', año_agrario, to_char(mes_fin_de_campaña, 'fm00')))::date - '60 day'::interval;
        fecha_fin := last_day(format('%s-%s-01', año_agrario+1, to_char(mes_fin_de_campaña, 'fm00'))::date) + '60 day'::interval;

        fecha_inicio_serie := ('1950' || TO_CHAR(fecha_inicio, '-MM-DD'))::date;
        fecha_fin_serie := fecha_inicio_serie + '630 day'::interval;

        RETURN QUERY
        WITH datos_raw AS (
            SELECT erd.fecha AS fecha_original, erd.tmax, erd.tmin, erd.prcp, erd.rad
            FROM estacion_registro_diario_completo erd
            WHERE erd.omm_id = id_estacion AND (erd.fecha BETWEEN fecha_inicio AND fecha_fin)
        ), datos_ordenados AS (
            SELECT row_number() OVER (ORDER BY dc.fecha_original ASC) AS row_number, dc.fecha_original,
                   dc.tmax, dc.tmin, dc.prcp, dc.rad
            FROM datos_raw dc
        ), fechas_ordenadas AS (
            SELECT row_number() OVER (ORDER BY fechas_generadas ASC) AS row_number, fechas_generadas::date
            FROM generate_series( fecha_inicio_serie, fecha_fin_serie, '1 day'::interval) fechas_generadas
        )
        SELECT fo.fechas_generadas, datos.fecha_original, datos.tmax, datos.tmin, datos.prcp, datos.rad
        FROM datos_ordenados datos LEFT JOIN fechas_ordenadas fo ON fo.row_number = datos.row_number;
    END
$$ LANGUAGE plpgsql;