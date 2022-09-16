#!/bin/bash

set -e

echo ""
echo "----- Install sccache -----"
mkdir -p $HOME/.local/bin
curl -L https://github.com/mozilla/sccache/releases/download/v0.2.15/sccache-v0.2.15-x86_64-unknown-linux-musl.tar.gz | tar xz
mv -f sccache-v0.2.15-x86_64-unknown-linux-musl/sccache $HOME/.local/bin/sccache
chmod +x $HOME/.local/bin/sccache
echo "$HOME/.local/bin" >> $GITHUB_PATH
echo 'SCCACHE_CACHE_SIZE="20G"' >> $GITHUB_ENV
mkdir -p /home/runner/.cache/sccache
echo ""

echo "----- Set up dynamic variables -----"
export PG_VER=$(echo ${POSTGRES_VERSION} | cut -d '-' -f2)
echo "PG_VER=$PG_VER" >> $GITHUB_ENV
echo "MAKEFLAGS=$MAKEFLAGS -j $(grep -c ^processor /proc/cpuinfo)" >> $GITHUB_ENV
cat $GITHUB_ENV
echo ""

echo "----- Remove old postgres -----"
sudo apt remove -y postgres*
echo ""

echo "----- Set up PostgreSQL Apt repository -----"
sudo apt-get install -y wget gnupg
sudo sh -c 'echo "deb http://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" > /etc/apt/sources.list.d/pgdg.list'
wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | sudo apt-key add -
sudo apt-get update -y -qq --fix-missing
echo ""

echo "----- Install system dependencies and PostgreSQL version $PG_VER -----"
sudo apt-get install -y \
    clang-10 \
    llvm-10 \
    clang \
    gcc \
    make \
    build-essential \
    libz-dev \
    zlib1g-dev \
    strace \
    libssl-dev \
    pkg-config \
    postgresql-$PG_VER \
    postgresql-server-dev-$PG_VER
echo ""

echo "----- Set up Postgres permissions -----"
sudo chmod a+rwx `/usr/lib/postgresql/$PG_VER/bin/pg_config --pkglibdir` `/usr/lib/postgresql/$PG_VER/bin/pg_config --sharedir`/extension /var/run/postgresql/
echo ""

echo "----- Set up JDK and Maven -----"
export JAVA_HOME=$JAVA_HOME_17_X64
echo "JAVA_HOME=$JAVA_HOME_17_X64" >> $GITHUB_ENV
echo ""

echo "----- Set up ecs-cli -----"
# TODO: Hopefully https://github.com/aws/amazon-ecs-cli/pull/1150 gets merged and can use mainline
sudo curl -Lo /usr/local/bin/ecs-cli https://ansilo-dev-tmp.s3.ap-southeast-2.amazonaws.com/ecs-cli 
sudo chmod +x /usr/local/bin/ecs-cli 
ecs-cli configure --cluster dev-cluster --region ap-southeast-2
echo ""

echo "----- Set up awscli -----"
curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
unzip awscliv2.zip
sudo ./aws/install --update
echo ""

echo "----- Install node.js and npm -----"
curl -fsSL https://deb.nodesource.com/setup_16.x | sudo -E bash - 
sudo apt-get install -y nodejs 
sudo npm install -g npm@latest
echo ""

echo "----- Print env -----"
env
echo ""

echo "----- Get cargo version -----"
cargo --version
echo ""

echo "----- Start sccache server -----"
sccache --start-server
echo ""

echo "----- Print sccache stats (before run) -----"
sccache --show-stats
echo ""

echo "----- Install cargo-pgx -----"
cargo install cargo-pgx --version 0.5.0-beta.0
echo ""

echo "----- Run 'cargo pgx init' against system-level postgres ------"
cargo pgx init --pg$PG_VER /usr/lib/postgresql/$PG_VER/bin/pg_config
echo ""
