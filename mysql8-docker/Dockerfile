# Use the official MySQL 8.0 image as the base image
FROM mysql:8.0

# Set the default build argument to "primary"
ARG build_type=primary

## Set stanza for mysqld
RUN echo "[mysqld]" >> /etc/mysql/conf.d/docker.cnf

# Set up the primary database
COPY primary/primary.sql /docker-entrypoint-initdb.d/
RUN echo "server-id=1" >> /etc/mysql/conf.d/docker.cnf

# Enable GTID replication
RUN echo "gtid_mode=ON" >> /etc/mysql/conf.d/docker.cnf
RUN echo "enforce_gtid_consistency=true" >> /etc/mysql/conf.d/docker.cnf

#RUN sed -i 's/server-id=1/server-id=101/g' /etc/mysql/conf.d/docker.cnf
# Set up the replica database
COPY replica/replica.sql /docker-entrypoint-initdb.d/
RUN if [ "$build_type" = "replica" ]; then \
        echo "server-id=2" >> /etc/mysql/conf.d/docker.cnf; \
    fi
RUN if [ "$build_type" = "replica" ]; then \
        echo "read_only=1" >> /etc/mysql/conf.d/docker.cnf; \
    fi    
RUN echo "log_slave_updates=ON" >> /etc/mysql/conf.d/docker.cnf
RUN echo "relay_log=/var/lib/mysql/mysql-relay-bin" >> /etc/mysql/conf.d/docker.cnf
RUN echo "relay_log_index=/var/lib/mysql/mysql-relay-bin.index" >> /etc/mysql/conf.d/docker.cnf
RUN echo "master_info_repository=TABLE" >> /etc/mysql/conf.d/docker.cnf
RUN echo "relay_log_info_repository=TABLE" >> /etc/mysql/conf.d/docker.cnf
# RUN echo "replicate_do_db=test" >> /etc/mysql/conf.d/docker.cnf
# RUN echo "replicate_ignore_db=mysql" >> /etc/mysql/conf.d/docker.cnf
# RUN echo "replicate_wild_ignore_table=mysql.%" >> /etc/mysql/conf.d/docker.cnf
# RUN echo "replicate_wild_ignore_table=performance_schema.%" >> /etc/mysql/conf.d/docker.cnf
# RUN echo "replicate_wild_ignore_table=information_schema.%" >> /etc/mysql/conf.d/docker.cnf

# Expose the MySQL port
EXPOSE 3306