#!/bin/bash
sed -i -e "s/archive.ubuntu.com/jp.archive.ubuntu.com/g" /etc/apt/sources.list
apt update
apt -y install openssh-server openssh-client gdb vim net-tools iproute2 less iputils-ping python python-pip libcurl4 git
pip install psutil paramiko lockfile

