FROM debian:jessie

RUN apt-get update -y && apt-get install mysql-client openssl -y
ADD https://github.com/sysown/proxysql/releases/download/v1.4.4/proxysql_1.4.4-dbg-debian8_amd64.deb /tmp/proxysql.deb
RUN dpkg -i /tmp/proxysql.deb && rm /tmp/proxysql.deb
COPY cnf/proxysql.cnf /etc/proxysql.cnf
COPY cnf/my.proxysql.cnf /root/.my.cnf
CMD ["proxysql", "-f", "--idle-threads"]
