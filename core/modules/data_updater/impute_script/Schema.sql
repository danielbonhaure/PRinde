DROP INDEX IF EXISTS erdi_index;
DROP MATERIALIZED VIEW IF EXISTS estacion_registro_diario_completo;
DROP TABLE IF EXISTS estacion_radiacion_diaria;
DROP TABLE IF EXISTS estacion_registro_diario_imputado;

CREATE TABLE estacion_registro_diario_imputado AS TABLE estacion_registro_diario WITH NO DATA;
ALTER TABLE estacion_registro_diario_imputado ADD CONSTRAINT estacion_registro_diario_imputado_pkey PRIMARY KEY (omm_id, fecha);
GRANT ALL ON TABLE estacion_registro_diario_imputado TO postgres;

CREATE TABLE estacion_radiacion_diaria (
	omm_id integer NOT NULL,
	fecha date NOT NULL,
	rad double precision,
	CONSTRAINT estacion_radiacion_diaria_pkey PRIMARY KEY (omm_id, fecha)
);
GRANT ALL ON TABLE estacion_radiacion_diaria TO postgres;

CREATE MATERIALIZED VIEW estacion_registro_diario_completo AS
(
	SELECT erd.omm_id, erd.fecha, coalesce(erdi.tmax, erd.tmax) tmax, coalesce(erdi.tmin, erd.tmin) tmin, coalesce(erdi.prcp, erd.prcp) prcp, rad, erd.helio, erd.nub
	FROM estacion_registro_diario erd
	FULL OUTER JOIN estacion_registro_diario_imputado erdi ON erdi.omm_id = erd.omm_id AND erdi.fecha = erd.fecha
	LEFT JOIN estacion_radiacion_diaria rad ON erd.omm_id = rad.omm_id AND erd.fecha = rad.fecha
	ORDER BY erd.fecha
);
ALTER MATERIALIZED VIEW estacion_registro_diario_completo OWNER TO postgres;
CREATE INDEX erdi_index ON estacion_registro_diario_completo (omm_id, fecha);

--SIN LO SIGUIENTE LLEGÓ A OCURRIR QUE SE DUPLICARON REGISTROS, LO QUE DERIVÓ EN LA FALLA LA IMPUTACIÓN!
ALTER TABLE estacion_registro_diario DROP CONSTRAINT IF EXISTS estacion_registro_diario_pkey;
ALTER TABLE estacion_registro_diario ADD CONSTRAINT estacion_registro_diario_pkey PRIMARY KEY (omm_id, fecha);