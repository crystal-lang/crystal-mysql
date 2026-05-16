-- Init script for mysql:8.0 (default auth: caching_sha2_password)
-- Creates test users with various authentication plugins.

CREATE USER 'native_user'@'%' IDENTIFIED WITH mysql_native_password BY 'native_pass';
CREATE USER 'csha2_user'@'%' IDENTIFIED WITH caching_sha2_password BY 'csha2_pass';
CREATE USER 'sha256_user'@'%' IDENTIFIED WITH sha256_password BY 'sha256_pass';
CREATE USER 'nopass_user'@'%' IDENTIFIED WITH mysql_native_password;

GRANT ALL PRIVILEGES ON *.* TO 'native_user'@'%';
GRANT ALL PRIVILEGES ON *.* TO 'csha2_user'@'%';
GRANT ALL PRIVILEGES ON *.* TO 'sha256_user'@'%';
GRANT ALL PRIVILEGES ON *.* TO 'nopass_user'@'%';

FLUSH PRIVILEGES;
