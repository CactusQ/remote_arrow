# Remote Arrow
**Remote Arrow** is a custom server built on top of [Apache Arrow Flight ](https://arrow.apache.org/blog/2019/10/13/introducing-arrow-flight/ "Apache Arrow Flight ") that allows clients that support the **Apache Arrow Flight API** to store and access **Apache Arrow** Tables via remote procedure calls.

## Motivation

Imagine you have a very large dataset that needs to be accessed and even modified by multiple clients that are written in different languages. The clients may work in the context of a larger application, for example a web-based tool for machine learning where a **React App** serves as a UX for uploading and viewing datasets, and a separate **Python client** in the background needs fetches that data in batches for NN training.

It would be unhandy for your clients to each download the entire dataset and process them locally. This may cause a lot of redundant computations and a significant overhead for data transmissions (i.e. passing query results around and synchronizing work).

Instead, you may want one centralized location, i.e. a server, where all the data resides. That server may then receive all queries and subsequently make the results accessible to all clients.

### How it works

The **Remote Arrow** server accepts upload and query requests from any client, and makes them accessible to all other clients as **Flights**. A **Flight** is linked to an *Arrow Table* inside the server that can be downloaded or also queried against. Queries or modifications on Flights, will result in another **Arrow Table** that is yet again stored as a **Flight**. In other words, every dataset or query result is internally stored as an **Arrow Table**.

That way, different clients can **A) work on the same datasets** without downloading them separately and **B) without having to compute the query results locally**. Instead, clients can access the shared datasets and query results through one common node.



## Requirements and setup
### Tutorial

If you have Docker you can run the following command and follow the instructions on the terminal:
```
docker run --rm -p 8888:8888 hoangln1/remote_arrow:tutorial
```
<br>

Alternatively, you can download and run the notebook locally. <br>


### Running the server

**Clone the repository** and install pyarrow:
```pip3 install pyarrow```

Run the server (default host is **localhost:5005**):
```python3 server.py --host <address> --port <port>```


The class **RemoteDataset** allows Python clients to upload datasets to the server (*CSV, pandas df, PARQUET*) and execute remote procedure calls disguised as local methods from [PyArrow Table](https://arrow.apache.org/docs/python/generated/pyarrow.Table.html "PyArrow Table"). 

In other words, local function calls are overridden with RPCs, so that the exact same function is executed on the server, published as a Flight, and the result immediately returned to the calling client, creating the illusion of a local call.

### Examples:

```
from RemoteArrow import RemoteDataset

# Uploads file to the server, and connects object (table) to the resulting Flight
client_A = RemoteDataset("some_file.csv")

# Take first 4 rows (remote procedure call)
# The server will execute the query against the original dataset
# and return the result
res = client_A.take([0,1,2,3])

# print the result as pandas dataframe
print(res.to_pandas())

# The server will return 2 available flights:
# the original dataset + the result of the previous take call
client_A.list_flights()
```


A different client can access all the Flights now:

```
from RemoteArrow import RemoteDataset

# No filepath given, so client_B is not connected to any flight
client_B = RemoteDataset()

# List all flights
client_B.list_flights()

# Connects to Flight 0 (the original dataset)
client_B.connect(0)

# RPC call to sort. Server will store the result as a new Flight
age_sorted = client_B.sort_by("age")

# All 3 Tables are available as Flights (original, first 4 rows, sorted by age)
client_B.list_flights()

```



Note, that you do not need **RemoteDataset** if you want to implement clients in other languages.<br>
As long as you follow the [Apache Arrow Flight API](https://arrow.apache.org/docs/format/Flight.html "Apache Arrow Flight API"), you can access all Flights on the server.
For a more in-depth explanation and documentation refer to the **Jupyter Notebook Tutorial**.


