# CMSConsumer

1. # Run Cassandra Container locally
  docker run --name cassandra -p 9042:9042 -d cassandra:latest

2. # Open cqlsh in the cassandra container with
   docker exec -it cassandra cqlsh

3. # Create keyspace 
  CREATE KEYSPACE IF NOT EXISTS cmskeyspace  WITH REPLICATION = {'class':'SimpleStrategy', 'replication_factor':1};

4. # Use newly created keyspace
  use cmskeyspace;

5. # Create table products
 CREATE TABLE IF NOT EXISTS products (
    PogId bigint,
    Supc text,
    Brand text,
    Description text,
    Size text,
    Category text,
    SubCategory text,
    Country text,
    SellerCode text,
    PRIMARY KEY (PogId, Supc)

);


   
