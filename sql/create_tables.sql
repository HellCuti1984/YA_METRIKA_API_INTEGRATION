DROP TABLE IF EXISTS ads_cab;
CREATE TABLE ads_cab (
  id int NOT NULL AUTO_INCREMENT,
  direct_client_logins varchar(100) DEFAULT NULL,
  token varchar(100) DEFAULT NULL,
  PRIMARY KEY (id)
);

DROP TABLE IF EXISTS api_v1;
CREATE TABLE api_v1 (
  id int NOT NULL AUTO_INCREMENT,
  direct_id text DEFAULT NULL,
  domain_name text DEFAULT NULL,
  rubAdCost double DEFAULT NULL,
  rubAdCostPerVisit text DEFAULT NULL,
  clicks int DEFAULT NULL,
  visits int DEFAULT NULL,
  PRIMARY KEY (id)
);

DROP TABLE IF EXISTS logs_api;
CREATE TABLE logs_api (
  id int NOT NULL AUTO_INCREMENT,
  ClientID text,
  visitID text DEFAULT NULL,
  goalsID text DEFAULT NULL,
  tLastDirectPlatform text DEFAULT NULL,
  domain text DEFAULT NULL,
  adDirect text DEFAULT NULL,
  regionCity text DEFAULT NULL,
  deviceCategory text DEFAULT NULL,
  operatingSystemRoot text DEFAULT NULL,
  dateTime varchar(16) DEFAULT NULL,
  PRIMARY KEY (id)
);
