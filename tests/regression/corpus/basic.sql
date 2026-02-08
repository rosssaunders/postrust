CREATE TABLE accounts (id int8 PRIMARY KEY, email text);
INSERT INTO accounts VALUES (1, 'a@example.com'), (2, 'b@example.com');
SELECT count(*) FROM accounts;
