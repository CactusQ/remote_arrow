# Remote Arrow
**Remote Arrow** is a prototype [Apache Arrow Flight ](https://arrow.apache.org/blog/2019/10/13/introducing-arrow-flight/ "Apache Arrow Flight ")server that allows clients to store and modify **Apache Arrow** Tables via remote procedure calls.

### Motivation

Imagine you have a very large dataset that needs to be accessed and even modified by multiple clients that work together (e.g. a web-based application for viewing the data, and a separate python client that fetches part of the data for ML training).

It would be unhandy for all clients to download the large dataset, compute different query results locally and synchronize the results with each other. This may cause a lot of redundant computations and data transmissions.

Instead, you may want one centralized location, i.e. a server, where all the original data resides. That server can also receive and compute queries and subsequently make the results accessible to all clients.

### How it works

The server accepts dataset uploads and queries requests from clients, and makes them accessible to all connected clients as **Flights**. A **Flight** is a *PyArrow Table* that can be downloaded and remotely addressed as such (e.g. queried against). Queries or modifications on Flights, will result in another **PyArrow Table** that is yet again stored as a **Flight**.

That way, different clients can **A) work on the same datasets** without actually downloading them (all data resides on the server) and **B) without having to compute the query results again (and locally)** (queries and modifications are done by the server). Instead clients can download only the **Flights** they are interested in and store them locally if desired, for example to convert them to pandas data frames.



### Requirements and setup
## Docker (Jupyter Notebook Tutorial)

Run the following command and follow the instructions on the terminal:
```
docker run --rm -p 8888:8888 hoangln1/remote_arrow:tutorial
```
<br>


## Run server locally

**Clone the repository** and install pyarrow:
```pip3 install pyarrow```

Run the server (default host is **localhost:5005**):
```python3 server.py --host <address> --port <port>```


The class **RemoteDataset** allows Python clients to upload datasets to the server (*CSV, pandas df, PARQUET*) and execute remote procedure calls disguised as local methods from [PyArrow Table](https://arrow.apache.org/docs/python/generated/pyarrow.Table.html "PyArrow Table"). 

In other words, local function calls are overridden with RPCs, so that the exact same function is executed on the server, published as a Flight, and the result immediately returned to the calling client, creating the illusion of a local call.

Example:

```
from RemoteDataset import RemoteDataset

# Uploads file to the server, and connects object (table) to the resulting Flight
table = RemoteDataset("some_file.csv")

# Take first 4 rows (remote procedure call)
# The server will execute the query against the original dataset
# and return the result
res = table.take([0,1,2,3])

# print the result as pandas dataframe
print(res.to_pandas())

# The server will return 2 available flights:
# the original dataset + the result of the previous take call
table.list_flights()
```


A different client may access the Flights now:

```
from RemoteDataset import RemoteDataset

# No filepath given, so table_b is not connected to any flight
table_b = RemoteDataset()

# List all flights incl. their id
table_b.list_flights()

# Connects to Flight with id: 0 (original dataset)
table_b.connect(0)

# RPC call to sort. Server will store the result as a new Flight
age_sorted = table_b.sort_by("age")

# All 3 Tables are available as Flights (original, first 4 rows, sorted by age)
table_b.list_flights()

```



Note, that you do not need **RemoteDataset** if you want to implement clients in other languages or frameworks. As long as you follow the [Apache Arrow Flight API](https://arrow.apache.org/docs/format/Flight.html "Apache Arrow Flight API"), you can access all Flights on the server, though without the remote procedure call wrapping.


