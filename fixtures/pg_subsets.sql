--
-- PostgreSQL database dump
--

CREATE TABLE "users" (
  id UUID PRIMARY KEY NOT NULL,
  first_name character varying(50) NOT NULL,
  email character varying(255) NOT NULL,
  administrator boolean NOT NULL
);

INSERT INTO "users" VALUES ('a1fbd1e4-6660-11ed-a7a1-af628227c989', 'Jimmy', 'jimmy@hellofresh.com', true);
INSERT INTO "users" VALUES ('9e1f48ee-6660-11ed-a476-13572d3d8c7d', 'Kairos', 'kairos@hellofresh.com', true);
INSERT INTO "users" VALUES ('9ac03f0a-6660-11ed-85cc-0f46f8189a84', 'Nutmeg', 'nutmeg@hellofresh.com', true);
INSERT INTO "users" VALUES ('95949b2a-6660-11ed-b8e6-ab0136b45676', 'Puppy', 'puppy@hellofresh.com', false);
INSERT INTO "users" VALUES ('90c91eea-6660-11ed-a24d-b3bc8fe05281', 'Patito', 'patito@hellofresh.com', false);

--
-- PostgreSQL database dump complete
--
