-- Init script for mysql:9.0 (caching_sha2_password only, native removed)
-- Creates test user with default auth (caching_sha2_password).

CREATE USER 'csha2_user'@'%' IDENTIFIED BY 'csha2_pass';

GRANT ALL PRIVILEGES ON *.* TO 'csha2_user'@'%';

FLUSH PRIVILEGES;
