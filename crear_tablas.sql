CREATE TABLE public.estadisticas_cines (
    provincia character varying,
    pantallas integer,
    butacas integer,
    "cantidad_de_espacios_INCAA" integer,
    fecha_carga date
);


ALTER TABLE public.estadisticas_cines OWNER TO postgres;

--
-- TOC entry 201 (class 1259 OID 18035)
-- Name: estadisticas_general; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.estadisticas_general (
    descripcion character varying,
    cantidad integer,
    fecha_carga date
);


ALTER TABLE public.estadisticas_general OWNER TO postgres;

--
-- TOC entry 202 (class 1259 OID 18041)
-- Name: locaciones; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.locaciones (
    id bigint,
    id_provincia integer,
    cod_localidad integer,
    provincia character varying,
    localidad character varying,
    nombre character varying,
    domicilio character varying,
    codigo_postal character varying,
    mail character varying,
    web character varying,
    fuente character varying,
    categoria character varying,
    telefono character varying,
    id_departamento integer,
    fecha_carga date
);


ALTER TABLE public.locaciones OWNER TO postgres;

--
-- TOC entry 2859 (class 1259 OID 18034)
-- Name: ix_estadisticas_cines_provincia; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX ix_estadisticas_cines_provincia ON public.estadisticas_cines USING btree (provincia);


--
-- TOC entry 2860 (class 1259 OID 18047)
-- Name: ix_locaciones_id; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX ix_locaciones_id ON public.locaciones USING btree (id);


-- Completed on 2022-01-04 02:34:50

--
-- PostgreSQL database dump complete
--

