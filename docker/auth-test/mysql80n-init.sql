-- Init script for mysql:8.0 with --default-authentication-plugin=mysql_native_password
-- Creates test users to trigger auth switch scenarios.

CREATE USER 'csha2_user'@'%' IDENTIFIED WITH caching_sha2_password BY 'csha2_pass';
CREATE USER 'native_user'@'%' IDENTIFIED WITH mysql_native_password BY 'native_pass';

GRANT ALL PRIVILEGES ON *.* TO 'csha2_user'@'%';
GRANT ALL PRIVILEGES ON *.* TO 'native_user'@'%';

FLUSH PRIVILEGES;
