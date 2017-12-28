--
-- PostgreSQL database dump
--

-- Dumped from database version 9.6.4
-- Dumped by pg_dump version 9.6.6

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SET check_function_bodies = false;
SET client_min_messages = warning;
SET row_security = off;

--
-- Name: plpgsql; Type: EXTENSION; Schema: -; Owner: 
--

CREATE EXTENSION IF NOT EXISTS plpgsql WITH SCHEMA pg_catalog;


--
-- Name: EXTENSION plpgsql; Type: COMMENT; Schema: -; Owner: 
--

COMMENT ON EXTENSION plpgsql IS 'PL/pgSQL procedural language';


SET search_path = public, pg_catalog;

SET default_tablespace = '';

SET default_with_oids = false;

--
-- Name: user; Type: TABLE; Schema: public; Owner: hello
--

CREATE TABLE "user" (
    id integer NOT NULL,
    username character varying(50) NOT NULL,
    email character varying(255) NOT NULL,
    active boolean NOT NULL,
    gender character(1)
);


ALTER TABLE "user" OWNER TO hello;

--
-- Name: user_id_seq; Type: SEQUENCE; Schema: public; Owner: hello
--

CREATE SEQUENCE user_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE user_id_seq OWNER TO hello;

--
-- Name: user_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: hello
--

ALTER SEQUENCE user_id_seq OWNED BY "user".id;


--
-- Name: user id; Type: DEFAULT; Schema: public; Owner: hello
--

ALTER TABLE ONLY "user" ALTER COLUMN id SET DEFAULT nextval('user_id_seq'::regclass);


--
-- Data for Name: user; Type: TABLE DATA; Schema: public; Owner: hello
--

INSERT INTO "user" VALUES (1, 'wbo', 'wbo@hellofresh.com', true, 'm');
INSERT INTO "user" VALUES (2, 'kp', 'kp@hellofresh.com', true, NULL);
INSERT INTO "user" VALUES (3, 'lp', 'lp@hellofresh.com', false, 'f');


--
-- Name: user_id_seq; Type: SEQUENCE SET; Schema: public; Owner: hello
--

SELECT pg_catalog.setval('user_id_seq', 3, true);


--
-- Name: user user_pkey; Type: CONSTRAINT; Schema: public; Owner: hello
--

ALTER TABLE ONLY "user"
    ADD CONSTRAINT user_pkey PRIMARY KEY (id);


--
-- PostgreSQL database dump complete
--

