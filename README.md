`docker run -d -p 1525:1521 -e ORACLE_PASSWORD=oracle -e APP_USER=admin -e APP_USER_PASSWORD=password gvenzl/oracle-xe`
`docker exec -it <nom du conteneur> /bin/bash`
`sqlplus sys/oracle@//localhost/XEPDB1 as sysdba`