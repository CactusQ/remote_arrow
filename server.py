import argparse
import ast

import threading
import time
import os

import pyarrow as pa
import pyarrow.flight as paf
import pyarrow.parquet as pq

from RemoteArrow import RemoteDataset

class FlightServer(paf.FlightServerBase):
    def __init__(self, host="localhost", location=None,
                 tls_certificates=None, verify_client=False,
                 root_certificates=None, auth_handler=None):

        super(FlightServer, self).__init__(
            location, auth_handler, tls_certificates, verify_client,
            root_certificates)
        
        # Assign IDs incrementally for each dataset (file) that has been uploaded (doPut) or query (doGet)
        self.id_to_key = {}
        self.id_counter = 0

        # Dictionary with (key: PyArrow.Table)
        self.flights = {}
        self.datasets_path = os.getcwd()+"/datasets/"

        for filename in os.listdir(self.datasets_path):
             if filename.endswith(".parquet"):
                # Generate descriptor from file name and cast to a key
                name, _parquet_suffix = filename.split(".")
                desc = pa.flight.FlightDescriptor.for_path(name)
                key = FlightServer.descriptor_to_key(desc)
                self.flights[key] = pq.read_table(self.datasets_path+filename, memory_map=True)
                print(f"Loaded from disk to Flight {self.id_counter}: {filename}")
                self.store_key(key)

        self.host = host

    @classmethod
    def descriptor_to_key(self, descriptor):
        return (descriptor.descriptor_type.value, descriptor.command,
                tuple(descriptor.path or tuple()))
    
    def store_key(self, key):
        # IDs are internally handeled as strings
        self.id_to_key[str(self.id_counter)] = key
        self.id_counter += 1

    def _make_flight_info(self, key, descriptor, table):
        if descriptor.descriptor_type == paf.DescriptorType.UNKNOWN:
            return paf.FlightInfo(None, descriptor, None, 0, 0)

        location = paf.Location.for_grpc_tcp(
        self.host, self.port)
        endpoints = [paf.FlightEndpoint(repr(key), [location]), ]
        mock_sink = pa.MockOutputStream()
        stream_writer = pa.RecordBatchStreamWriter(
            mock_sink, table.schema)
        stream_writer.write_table(table)
        stream_writer.close()
        data_size = mock_sink.size()
        return paf.FlightInfo(table.schema,
                                         descriptor, endpoints,
                                         table.num_rows, data_size)

    def list_flights(self, context, criteria):
        for id, key in self.id_to_key.items():
            table = self.flights[key]
            if key == None:
                descriptor = paf.FlightDescriptor(descriptor_type = paf.DescriptorType.UNKNOWN)
            elif key[1] is not None:
                descriptor = paf.FlightDescriptor.for_command(key[1])
            else:
                descriptor = paf.FlightDescriptor.for_path(*key[2])
            yield self._make_flight_info(key, descriptor, table)


    def get_flight_info(self, context, descriptor):
        key = FlightServer.descriptor_to_key(descriptor)
        if descriptor.descriptor_type == paf.DescriptorType.CMD \
            and key not in self.flights:
                if not self.do_command(key, descriptor):
                    return KeyError('Could not execute command.')

        if key in self.flights:
            table = self.flights[key]
            return self._make_flight_info(key, descriptor, table)
                       
        raise KeyError('Flight not found.')


    def do_command(self, new_key, descriptor) -> bool:
        decoded_str = descriptor.command.decode("utf-8")
        id, method, strargs, strkwargs = decoded_str.split(RemoteDataset.DELIMITER)
        args = tuple(ast.literal_eval(strargs).values())

        # Flatten tuples that only have 1 element
        if len(args) == 0:
            args = None
        elif len(args) == 1:
            args = args[0]

        kwargs = ast.literal_eval(strkwargs)
        kwargs = kwargs if len(kwargs) > 0 else None

        # print("str: ", decoded_str)
        # print("strargs:", strargs)
        # print("strkwargs:", strkwargs)
        # print("args:", args)
        # print("kwargs:", kwargs)
        key = self.id_to_key[id]

        # TODO: can this be done more elegantly?
        if args and kwargs:
            result = getattr(self.flights[key], method)(args, kwargs)
        elif args and kwargs is None:
            result = getattr(self.flights[key], method)(args)
        elif kwargs and args is None:
            result = getattr(self.flights[key], method)(kwargs)
        else:
            result = getattr(self.flights[key], method)()

        if type(result) == pa.Table:
            self.flights[new_key] = result
            self.store_key(new_key)
            return True
        else:
            return False

    def save_flight(self, id, filename) -> bool:
        if id in self.id_to_key and self.id_to_key[id] != None:
            key = self.id_to_key[id]
            table = self.flights[key]
            if not filename.endswith(".parquet"):
                filename += ".parquet"
            pq.write_table(table, self.datasets_path + filename)
            print("Saved dataset to: " + self.datasets_path + filename)
            return True
        else:
            return False

    def delete_flight(self, delete_id) -> bool:
        if delete_id in self.id_to_key and self.id_to_key[delete_id] != None:
            key = self.id_to_key[delete_id]
            self.id_to_key[delete_id] = None
            self.flights.pop(key)

            if paf.DescriptorType(key[0]) == paf.DescriptorType.PATH:
                filename = key[2][0].decode("utf-8")+'.parquet'
                for filepath in self.datasets_path:
                    if filepath.endswith(filename):
                        os.remove(filepath)
            return True
        else:
            return False
    
    def clear_flights(self):
        self.flights = {}
        self.id_to_key = {}
        self.id_counter = 0

    def do_put(self, context, descriptor, reader, writer):
        key = FlightServer.descriptor_to_key(descriptor)
        filename = key[2][0].decode("utf-8")+'.parquet'
        filepath = self.datasets_path + filename

        if key not in self.flights:
            with pq.ParquetWriter(filepath, reader.schema) as writer:
                for chunk in reader:
                    writer.write_table(pa.Table.from_batches([chunk.data]))
    
            self.flights[key] = pq.read_table(self.datasets_path+filename, memory_map=True)
            self.store_key(key)
            print("Saved dataset as: " + self.datasets_path + filename)
            writer.close()
        else:
            raise KeyError(f"Flight with same filename ({filepath}) already exists. Rename to avoid ambiguity.")

    def do_get(self, context, ticket):
        key = ast.literal_eval(ticket.ticket.decode())
        if key not in self.flights:
            return None
        elif paf.DescriptorType(key[0]) == paf.DescriptorType.CMD:
            return paf.RecordBatchStream(self.flights[key])

        # Read file in batches from local file instead from table
        if len(key[2]) == 0: return
        filename = key[2][0].decode("utf-8")
        reader = pq.ParquetFile(self.datasets_path+filename+'.parquet', memory_map=True)
        return paf.GeneratorStream(
            reader.schema_arrow, reader.iter_batches())

    def list_actions(self, context):
        return [
            ("clear", "Clear (delete) all flights. Parquet files are not affected. "),
            ("shutdown", "Shut down this server."),
            ("delete", "Delete a flight and corresponding .parquet file, if existing. [action.body == \"<FLIGHT_ID>\"]"),
            ("save", "Save a flight as .parquet file. [action.body == \"<FLIGHT_ID> <FILENAME>\"]"),
        ]

    def do_action(self, context, action):
        body_str = action.body.to_pybytes().decode("utf-8")
        if action.type == "clear":
            self.clear_flights()
            yield paf.Result(pa.py_buffer(b'Successfully cleared all flights'))

        elif action.type == "shutdown":
            yield paf.Result(pa.py_buffer(b'Shutdown!'))
            # Shut down on background thread to avoid blocking current
            threading.Thread(target=self._shutdown).start()

        elif action.type == "save":
            target_id, filename = body_str.split(" ")
            if self.save_flight(target_id, filename):
                msg = f"Successfully saved flight {target_id} as {filename}"
            else:
                msg = f"Could not save {target_id}. Flight not found."
            yield paf.Result(msg.encode("utf-8"))

        elif action.type == "delete":
            target_id = body_str
            if self.delete_flight(target_id):
                msg = f"Successfully deleted flight {target_id}"
            else:
                msg = f"Could not delete {target_id}. Flight not found."
            yield paf.Result(msg.encode("utf-8"))
        else:
            raise KeyError("Unknown action {!r}".format(action.type))
        
    def _shutdown(self):
        """Shut down after a delay."""
        print("Server is shutting down...")
        time.sleep(2)
        self.shutdown()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", type=str, default="localhost",
                        help="Address or hostname to listen on")
    parser.add_argument("--port", type=int, default=5005,
                        help="Port number to listen on")
    args = parser.parse_args()
    scheme = "grpc+tcp"

    location = "{}://{}:{}".format(scheme, args.host, args.port)
    server = FlightServer(args.host, location)
    print("Server online: ", location)
    server.serve()

if __name__ == '__main__':
    main()