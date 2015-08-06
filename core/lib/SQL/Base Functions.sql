DROP FUNCTION IF EXISTS pr_create_campaigns(int, varchar, varchar, varchar, varchar);
DROP FUNCTION IF EXISTS pr_crear_serie(int, date, date, date, int);
DROP FUNCTION IF EXISTS pr_año_agrario(date);
DROP FUNCTION IF EXISTS is_leap(int);

/* Función que determina si un año es bisiesto. */
CREATE OR REPLACE FUNCTION is_leap(year integer)
RETURNS BOOLEAN AS $$
	SELECT ($1 % 4 = 0) AND (($1 % 100 <> 0) OR ($1 % 400 = 0))
$$ LANGUAGE sql;

CREATE OR REPLACE FUNCTION pr_año_agrario(fecha date)
RETURNS DOUBLE PRECISION AS $$
    SELECT CASE WHEN(EXTRACT(MONTH FROM $1) < 5)
                      THEN EXTRACT(YEAR FROM $1)-1
                ELSE EXTRACT(YEAR FROM $1)
           END
$$ LANGUAGE sql;

CREATE OR REPLACE FUNCTION pr_campañas_completas(omm_id int)
RETURNS TABLE (pr_year INT) AS $$
    WITH count_agrario AS (
        SELECT DISTINCT pr_año_agrario(fecha)::int AS pr_year, COUNT(1) AS count
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


CREATE OR REPLACE FUNCTION pr_crear_serie(omm_id int, fecha_inicio date, fecha_inflexion date, fecha_fin date, year_inflexion int)
RETURNS TABLE (fecha date, fecha_original date, tmax numeric, tmin numeric, prcp numeric, rad numeric)
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


CREATE OR REPLACE FUNCTION pr_create_campaigns(omm_id integer, camp_start varchar, curr_date varchar,
                                               camp_end varchar, output_path varchar)
RETURNS VOID
AS $$
    DECLARE
        id_estacion ALIAS FOR $1;
        campaign_start ALIAS FOR $2;
        curr_date ALIAS FOR $3;
        campaign_end ALIAS FOR $4;
        output_folder ALIAS FOR $5;
        
        current_year INTEGER;
        curr_monthday VARCHAR;
        
        estacion RECORD;
        path VARCHAR;
        command VARCHAR;

        loop_year RECORD;
        data_estacion RECORD;

    BEGIN
        -- Convertimos las fechas de los parámetros a Dates de SQL.
        curr_date := curr_date::date;
        campaign_start := campaign_start::date;
        campaign_end := campaign_end::date;

    
        IF ( campaign_start >= campaign_end ) THEN
                RAISE EXCEPTION 'Data end date should be greater than data start date.';
        END IF;

        IF ( NOT curr_date BETWEEN campaign_start AND campaign_end ) THEN
                RAISE EXCEPTION 'Current date isn''t between data start date and data end date.';
        END IF;

        -- Extraemos el año de la fecha que se pasa como actual y el mes-día.
        current_year := EXTRACT(YEAR FROM curr_date::date);
        curr_monthday := TO_CHAR(curr_date::date, 'MM-DD');

        -- Escribimos un archivo con la información de la estación.
        path := output_folder || '/_' || omm_id || '.csv';

        command := 'COPY ( SELECT * FROM estacion WHERE omm_id = ' || id_estacion || ') TO ''' || path || ''' HEADER CSV NULL '''' DELIMITER E''\t''';
        EXECUTE command;

        -- Escribimos un archivo por cada serie climática combinada.
        FOR loop_year IN (SELECT pr_year FROM pr_campañas_completas(id_estacion))
        LOOP
            -- Salteamos el año actual.
            CONTINUE WHEN loop_year.pr_year >= current_year;

            path := output_folder || '/' || id_estacion  || ' - ' || loop_year.pr_year|| '.csv';

            command := 'COPY ( SELECT * FROM pr_crear_serie( ' || id_estacion || ', ''' || campaign_start || '''::date, ''' ||
                                 curr_date || '''::date, ''' || campaign_end || '''::date, ' || loop_year.pr_year || ' ) ) ' ||
                                 'TO ''' || path || ''' HEADER CSV NULL '''' DELIMITER E''\t''';

            EXECUTE command;
        END LOOP;
    END
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION pr_historic_series(omm_id integer, output_path varchar)
RETURNS VOID
AS $$
    DECLARE
        id_estacion ALIAS FOR $1;
        output_folder ALIAS FOR $2;

        estacion RECORD;
        path VARCHAR;
        command VARCHAR;

        loop_year RECORD;
        data_estacion RECORD;

    BEGIN
        -- Escribimos un archivo con la información de la estación.
        path := output_folder || '/_' || omm_id || '.csv';

        command := 'COPY ( SELECT * FROM estacion WHERE omm_id = ' || id_estacion || ') TO ''' || path ||
                   ''' HEADER CSV NULL '''' DELIMITER E''\t''';
        EXECUTE command;

        -- Escribimos un archivo por cada año agrario completo.
        FOR loop_year IN (SELECT pr_year FROM pr_campañas_completas(id_estacion))
        LOOP
            path := output_folder || '/' || id_estacion  || ' - ' || loop_year.pr_year|| '.csv';

            command := 'COPY ( SELECT * FROM pr_serie_agraria( ' || id_estacion || ', ' || loop_year.pr_year || ' ) ) ' ||
                                 'TO ''' || path || ''' HEADER CSV NULL '''' DELIMITER E''\t''';

            EXECUTE command;
        END LOOP;
    END
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION pr_serie_agraria(omm_id int, year_agrario int)
RETURNS TABLE (fecha date, fecha_original date, tmax numeric, tmin numeric, prcp numeric, rad numeric)
AS $$
    DECLARE
        id_estacion ALIAS FOR $1;
        año_agrario ALIAS FOR $2;

        fecha_inicio DATE;
        fecha_fin DATE;
        fecha_inicio_serie DATE;
        fecha_fin_serie DATE;
    BEGIN
        fecha_inicio := (año_agrario || '-03-01')::date;
        fecha_fin := (año_agrario || '-04-30')::date + '395 day'::interval;

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