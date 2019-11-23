#!/bin/bash
sed -i -e "s/archive.ubuntu.com/jp.archive.ubuntu.com/g" /etc/apt/sources.list
apt update
apt -y install openssh-server openssh-client gdb vim net-tools iproute2 less iputils-ping python python-pip libcurl4 git language-pack-en
apt -y install g++ flex bison libreadline-dev libcurl4-openssl-dev libxml2-dev libz-dev libbz2-dev libssl-dev tmux
pip install psutil paramiko lockfile

CFLAGS=-O0 ./configure --prefix=/home/gpadmin/greenplum-db-devel --disable-orca --disable-gpfdist --disable-pxf --disable-gpcloud --enable-debug --without-zstd