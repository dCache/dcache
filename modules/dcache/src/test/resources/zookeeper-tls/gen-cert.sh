#!/bin/sh
#
# Generates TLS certificates for ZooKeeper and the dCache Curator client.
# These are DISPOSABLE TEST CERTIFICATES only. Do NOT use in production.
#
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

# CA
openssl genrsa -out ca-key.pem 4096
openssl req -new -x509 -days 3650 -key ca-key.pem -out ca-cert.pem -subj "/CN=ZooKeeper-Test-CA"

# ZooKeeper server PEM credentials
# ZooKeeper's PEM keystore loader expects cert and key in a single combined file
openssl genrsa -out zookeeper-key.pem 2048
openssl req -new -key zookeeper-key.pem -out zookeeper.csr -subj "/CN=localhost"
openssl x509 -req -days 3650 -in zookeeper.csr \
    -CA ca-cert.pem -CAkey ca-key.pem -CAcreateserial -out zookeeper-cert.pem
cat zookeeper-cert.pem zookeeper-key.pem > zookeeper-combined.pem

# Curator client PEM credentials
openssl genrsa -out curator-key.pem 2048
openssl req -new -key curator-key.pem -out curator.csr -subj "/CN=dcache-client"
openssl x509 -req -days 3650 -in curator.csr \
    -CA ca-cert.pem -CAkey ca-key.pem -CAcreateserial -out curator-cert.pem

# CA directory for OpensslCertChainValidator (requires hash-named files)
mkdir -p certificates
hash=$(openssl x509 -noout -subject_hash -in ca-cert.pem)
cp ca-cert.pem certificates/${hash}.0

# clean up files that not needed for testing
rm -f ca-key.pem \
      zookeeper-key.pem zookeeper-cert.pem zookeeper.csr \
      curator.csr \
      ca-cert.srl
