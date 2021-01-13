# local

export $(grep -v '^#' .env | xargs)

# VM setup

# ripple can build on debian 9 but not debian 10

IMAGE=debian-9-stretch-v20201216
ZONE=europe-north1-a
INSTANCE=kirs-mysql-m1

gcloud beta compute --project $PROJECT instances delete $INSTANCE

gcloud beta compute --project $PROJECT instances create $INSTANCE --zone=$ZONE --machine-type=n1-standard-8 --subnet=default --network-tier=PREMIUM --maintenance-policy=MIGRATE --service-account=787919928393-compute@developer.gserviceaccount.com --scopes=https://www.googleapis.com/auth/cloud-platform --image=$IMAGE --image-project=debian-cloud --boot-disk-size=200GB --boot-disk-type=pd-standard --boot-disk-device-name=instance-1 --reservation-affinity=any --boot-disk-auto-delete

gcloud beta compute --project $PROJECT ssh $INSTANCE

# docker

sudo apt-get update

sudo apt-get install \
 apt-transport-https \
 ca-certificates \
 curl \
 gnupg-agent \
 software-properties-common

curl -fsSL https://download.docker.com/linux/debian/gpg | sudo apt-key add -

sudo add-apt-repository \
 "deb [arch=amd64] https://download.docker.com/linux/debian \
 $(lsb_release -cs) \
 stable"

sudo apt-get update
sudo apt-get install docker-ce docker-ce-cli containerd.io

sudo usermod -aG docker $USER

# relogin

# ripple

sudo apt-get install git-core pkg-config zip g++ zlib1g-dev unzip python libssl-dev default-jdk-headless libmariadbclient-dev
echo "deb [arch=amd64] http://storage.googleapis.com/bazel-apt stable jdk1.8" | sudo tee /etc/apt/sources.list.d/bazel.list
curl https://bazel.build/bazel-release.pub.gpg | sudo apt-key add -
sudo apt-get update && sudo apt-get install bazel

git clone https://github.com/google/mysql-ripple.git
cd mysql-ripple
bazel build :all
bazel test :all

docker run --network=host --name mysql-m1 -e MYSQL_ROOT_PASSWORD=shopify -e MYSQL_DATABASE=shopify -d mysql:5.7

# run local mysql

docker run -d --network=host --name mysql-m1-percona -e MYSQL_ALLOW_EMPTY_PASSWORD=yes percona:5.7-jessie \
 --server-id=1 \
 --log-bin=mysql-bin \
 --binlog-format=ROW \
 --sync-binlog=1 \
 --log-slave-updates=ON \
 --gtid-mode=ON \
 --enforce-gtid-consistency=ON \
 --character-set-server=utf8mb4 \
 --collation-server=utf8mb4_unicode_ci

docker run -d --network=host --name mysql-s1-percona -e MYSQL_ALLOW_EMPTY_PASSWORD=yes percona:5.7-jessie \
 --server-id=2 \
 --log-bin=mysql-bin \
 --binlog-format=ROW \
 --sync-binlog=1 \
 --log-slave-updates=ON \
 --gtid-mode=ON \
 --enforce-gtid-consistency=ON \
 --character-set-server=utf8mb4 \
 --collation-server=utf8mb4_unicode_ci \
 --port=3307

mysql -h 127.0.0.1 -P 3306 -u root

CHANGE MASTER TO MASTER_HOST='127.0.0.1', MASTER_USER='root', MASTER_PORT=51005, MASTER_LOG_FILE='binlog.000000', MASTER_LOG_POS= 3225712;

mysql -h 127.0.0.1 -P 3307 -u root -e "create database shopiglobo"
mysqldump -h 127.0.0.1 -P 3306 -u root shopiglobo | mysql -h 127.0.0.1 -P 3307 -u root shopiglobo

./mysql-ripple/bazel-bin/rippled -ripple_master_address 127.0.0.1 -ripple_master_port 3306

1. SHOW MASTER STATUS, see position
2. start slave with the dump
3. CHANGE MASTER TO ...

ripple is working!
things to hack on:

- what's up with MASTER_LOG_POS? can I omit it?
- try to log actual events
