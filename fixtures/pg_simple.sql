--
-- PostgreSQL database dump
--

CREATE TABLE "users" (
  id UUID PRIMARY KEY NOT NULL,
  username character varying(50) NOT NULL,
  email character varying(255) NOT NULL,
  active boolean NOT NULL,
  gender character(1),
  created_at timestamp
);

CREATE TABLE "orders" (
  id UUID PRIMARY KEY NOT NULL,
  user_id UUID NOT NULL,
  created_at timestamp,
  FOREIGN KEY (user_id) REFERENCES users(id)
);

CREATE TABLE "order_items" (
  id UUID PRIMARY KEY NOT NULL,
  order_id UUID NOT NULL,
  created_at timestamp,
  FOREIGN KEY (order_id) REFERENCES orders(id)
);


CREATE VIEW "users_view" AS SELECT id, username FROM "users" WHERE active=true;

INSERT INTO "users" VALUES ('0d60a85e-0b90-4482-a14c-108aea2557aa', 'wbo', 'wbo@hellofresh.com', true, 'm', '2017-01-01');
INSERT INTO "users" VALUES ('39240e9f-ae09-4e95-9fd0-a712035c8ad7', 'kp', 'kp@hellofresh.com', true, NULL, '2017-01-01');
INSERT INTO "users" VALUES ('9e4de779-d6a0-44bc-a531-20cdb97178d2', 'lp', 'lp@hellofresh.com', false, 'f', '2017-01-01');
INSERT INTO "users" VALUES ('66a45c1b-19af-4ab5-8747-1b0e2d79339d', 'il', 'il@hellofresh.com', true, 'm', '2017-01-01');

INSERT INTO "orders" VALUES ('b9bcd5e1-75e6-412d-be87-278003519717', '66a45c1b-19af-4ab5-8747-1b0e2d79339d', '2018-01-01');
INSERT INTO "orders" VALUES ('7ee31a7f-5140-483b-8ba1-fa8f116219c0', '66a45c1b-19af-4ab5-8747-1b0e2d79339d', '2018-01-01');
INSERT INTO "orders" VALUES ('dda290ff-6243-46d9-83cb-acbad41e936e', '66a45c1b-19af-4ab5-8747-1b0e2d79339d', '2018-01-01');
INSERT INTO "orders" VALUES ('453f4498-b4e0-485f-94fa-72f233bb7958', '9e4de779-d6a0-44bc-a531-20cdb97178d2', '2018-01-01');
INSERT INTO "orders" VALUES ('8bdf39d8-616c-45d4-826f-bad30cb4e1a3', '9e4de779-d6a0-44bc-a531-20cdb97178d2', '2018-01-01');
INSERT INTO "orders" VALUES ('f1f7c9c7-bdb7-4626-a5c9-44d8942e52dd', '39240e9f-ae09-4e95-9fd0-a712035c8ad7', '2018-01-01');
INSERT INTO "orders" VALUES ('e650ad64-f1e4-4f91-abea-ec1a70992926', '39240e9f-ae09-4e95-9fd0-a712035c8ad7', '2018-01-01');
INSERT INTO "orders" VALUES ('2b92734e-0e4c-11e8-ba89-0ed5f89f718b', '66a45c1b-19af-4ab5-8747-1b0e2d79339d', '2017-01-01');

INSERT INTO "order_items" VALUES ('7e4e057e-1709-11e8-b642-0ed5f89f718b', 'b9bcd5e1-75e6-412d-be87-278003519717', '2018-01-01');
INSERT INTO "order_items" VALUES ('dcad1150-1709-11e8-b642-0ed5f89f718b', '7ee31a7f-5140-483b-8ba1-fa8f116219c0', '2018-01-01');
INSERT INTO "order_items" VALUES ('d0d80524-174a-11e8-b642-0ed5f89f718b', '453f4498-b4e0-485f-94fa-72f233bb7958', '2018-01-01');
--
-- PostgreSQL database dump complete
--
