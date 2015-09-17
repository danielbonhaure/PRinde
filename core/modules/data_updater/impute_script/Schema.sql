DROP INDEX IF EXISTS erdi_index;
DROP MATERIALIZED VIEW IF EXISTS estacion_registro_diario_completo;
DROP TABLE IF EXISTS estacion_registro_diario_imputado;
DROP TABLE IF EXISTS estacion_radiacion_diaria;

--CREATE TABLE estacion_registro_diario_imputado AS (
--	SELECT * FROM estacion_registro_diario
--	WHERE omm_id IN (87585, 87548)
--	      AND (tmax ISNULL OR tmin ISNULL OR prcp ISNULL)
--);
CREATE TABLE estacion_registro_diario_imputado AS TABLE estacion_registro_diario WITH NO DATA;
ALTER TABLE estacion_registro_diario_imputado ADD CONSTRAINT estacion_registro_diario_imputado_pkey PRIMARY KEY (omm_id, fecha);
GRANT ALL ON TABLE estacion_registro_diario_imputado TO crcssa_user;

CREATE TABLE estacion_radiacion_diaria (
	omm_id integer NOT NULL,
	fecha date NOT NULL,
	rad double precision,
	CONSTRAINT estacion_radiacion_diaria_pkey PRIMARY KEY (omm_id, fecha)
);
GRANT ALL ON TABLE estacion_radiacion_diaria TO crcssa_user;

CREATE MATERIALIZED VIEW estacion_registro_diario_completo AS
(
	SELECT erd.omm_id, erd.fecha, coalesce(erdi.tmax, erd.tmax) tmax, coalesce(erdi.tmin, erd.tmin) tmin, coalesce(erdi.prcp, erd.prcp) prcp, rad, erd.helio, erd.nub
	FROM estacion_registro_diario erd
	FULL OUTER JOIN estacion_registro_diario_imputado erdi ON erdi.omm_id = erd.omm_id AND erdi.fecha = erd.fecha
	LEFT JOIN estacion_radiacion_diaria rad ON erd.omm_id = rad.omm_id AND erd.fecha = rad.fecha
	ORDER BY erd.fecha
);
ALTER MATERIALIZED VIEW estacion_registro_diario_completo OWNER TO crcssa_user;
CREATE INDEX erdi_index ON estacion_registro_diario_completo (omm_id, fecha);

--REFRESH MATERIALIZED VIEW estacion_registro_diario_completo;

--SELECT * FROM estacion_registro_diario_completo
--WHERE omm_id = 87548 AND fecha BETWEEN '1964-12-01'::date AND '2014-01-01'::date