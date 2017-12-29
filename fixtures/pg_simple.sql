--
-- PostgreSQL database dump
--

CREATE TABLE "users" (
    id UUID PRIMARY KEY NOT NULL,
    username character varying(50) NOT NULL,
    email character varying(255) NOT NULL,
    active boolean NOT NULL,
    gender character(1)
);

INSERT INTO "users" VALUES ('0d60a85e-0b90-4482-a14c-108aea2557aa', 'wbo', 'wbo@hellofresh.com', true, 'm');
INSERT INTO "users" VALUES ('39240e9f-ae09-4e95-9fd0-a712035c8ad7', 'kp', 'kp@hellofresh.com', true, NULL);
INSERT INTO "users" VALUES ('9e4de779-d6a0-44bc-a531-20cdb97178d2', 'lp', 'lp@hellofresh.com', false, 'f');
INSERT INTO "users" VALUES ('66a45c1b-19af-4ab5-8747-1b0e2d79339d', 'il', 'il@hellofresh.com', true, 'm');

--
-- PostgreSQL database dump complete
--
