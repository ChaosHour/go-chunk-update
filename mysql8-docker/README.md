# MySQL 8 Docker, with primary and replica

## How To use this project, a little prep work is needed

```Go
Working with Docker creds:

Create a file called .env in the root of the project and add the following:
MYSQL_ROOT_PASSWORD=s3cr3t

```

## Replica password

```bash
The replica password used for testing is in the replica.sql file. I would recommend changing this password with an alter user command.
An example of this is in the primary.sql file.
```

## To build and start the containers

```bash
make start
```

## To stop the containers and remove the containers and volumes

```bash
make stop
make clean
```

## Using the ~/.my.cnf

```bash
cat ~/.my.cnf
[client]
user=root
password=s3cr3t

[client_primary1]
host=192.168.50.50
port=3306

[client_replica1]
host=192.168.50.50
port=3307
```

## Interacting with the primary and replica

```bash

❯ mysql --defaults-group-suffix=_primary1 -e "select @@read_only"
+-------------+
| @@read_only |
+-------------+
|           0 |
+-------------+


❯ mysql --defaults-group-suffix=_replica1 -e "select @@read_only"
+-------------+
| @@read_only |
+-------------+
|           1 |
+-------------+


❯ mysql --defaults-group-suffix=_replica1 -e "select @@server_id"
+-------------+
| @@server_id |
+-------------+
|           2 |
+-------------+


❯ mysql --defaults-group-suffix=_primary1 -e "select @@server_id"
+-------------+
| @@server_id |
+-------------+
|           1 |
+-------------+


mysql --defaults-group-suffix=_replica1 -e "show slave status\G" | awk -v RS='\n ' '
{
    if ($1 ~ /Master_Host|Slave_IO_Running|Slave_SQL_Running|Seconds_Behind_Master|Retrieved_Gtid_Set|Executed_Gtid_Set/) {
        split($0, a, ": ");
        print a[1] ": " substr($0, index($0, a[2]));
    }
}'
                 Master_Host: 172.20.0.3
            Slave_IO_Running: Yes
           Slave_SQL_Running: Yes
       Seconds_Behind_Master: 0
Master_SSL_Verify_Server_Cert: No
     Slave_SQL_Running_State: Replica has read all relay log; waiting for more updates
          Retrieved_Gtid_Set: 35dacfbe-1c26-11f0-ab3a-3eaa1b6dc9dc:1-69
           Executed_Gtid_Set: 35dacfbe-1c26-11f0-ab3a-3eaa1b6dc9dc:1-69,
35e31a1e-1c26-11f0-b708-f6db8cba6bab:1-11
```
