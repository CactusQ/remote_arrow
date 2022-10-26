import argparse
import ast

import threading
import time
import os

import pyarrow as pa
import pyarrow.flight as paf
import pyarrow.parquet as pq

from RemoteDataset import RemoteDataset

from ast import literal_eval as make_tuple

class FlightServer(paf.FlightServerBase):
    def __init__(self, host="localhost", location=None,
                 tls_certificates=None, verify_client=False,
                 root_certificates=None, auth_handler=None):

        super(FlightServer, self).__init__(
            location, auth_handler, tls_certificates, verify_client,
            root_certificates)
        
        # Create mapping from id to key for readability
        self.id_to_key = {}
        self.id_counter = 0

        self.flights = {}
        for filepath in os.listdir(os.getcwd()):
             if filepath.endswith(".parquet"):
                # Generate descriptor from file name and cast to a key
                filename = os.path.basename(filepath).split('.')[0]
                desc = pa.flight.FlightDescriptor.for_path(filename)
                key = self.descriptor_to_key(desc)
                self.flights[key] = pq.read_table(filename+'.parquet', memory_map=True)
                self.store_key(key)      
                print("Loaded from disk: " + filename+'.parquet')
        print("List of flights:")
        print(self.id_to_key)
        self.host = host

    @classmethod
    def descriptor_to_key(self, descriptor):
        return (descriptor.descriptor_type.value, descriptor.command,
                tuple(descriptor.path or tuple()))
    
    def store_key(self, key):
        self.id_to_key[str(self.id_counter)] = key
        self.id_counter += 1  

    def _make_flight_info(self, key, descriptor, table):
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
            if key[1] is not None:
                descriptor = paf.FlightDescriptor.for_command(key[1])
            else:
                descriptor = paf.FlightDescriptor.for_path(*key[2])
            yield self._make_flight_info(key, descriptor, table)


    def get_flight_info(self, context, descriptor):
        key = FlightServer.descriptor_to_key(descriptor)
        if descriptor.descriptor_type == paf.DescriptorType.CMD \
            and key not in self.flights:
                if not self.do_command(key, descriptor):
                    return

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

        print("str: ", decoded_str)
        print("strargs:", strargs)
        print("strkwargs:", strkwargs)
        print("args:", args)
        print("kwargs:", kwargs)
        key = self.id_to_key[str(id)]

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
            

    def do_put(self, context, descriptor, reader, writer):
        key = FlightServer.descriptor_to_key(descriptor)
        print("Received put command with key:", key)
        self.flights[key] = reader.read_all()
        self.store_key(key)
        filepath = key[2][0].decode("utf-8")+'.parquet'
        pq.write_table(self.flights[key], filepath)
        print("Writing to " + filepath)

    def do_get(self, context, ticket):
        key = ast.literal_eval(ticket.ticket.decode())
        if key not in self.flights:
            return None
        elif key[0] == 2:
            return paf.RecordBatchStream(self.flights[key])

        if len(key[2]) == 0: return
        filename = key[2][0].decode("utf-8")
        reader = pq.ParquetFile(filename+'.parquet', memory_map=True)
        return paf.GeneratorStream(
            reader.schema_arrow, reader.iter_batches())

    def list_actions(self, context):
        return [
            ("clear", "Clear the stored flights."),
            ("shutdown", "Shut down this server."),
        ]

    def do_action(self, context, action):
        if action.type == "clear":
            raise NotImplementedError(
                "{} is not implemented.".format(action.type))
        elif action.type == "healthcheck":
            pass
        elif action.type == "shutdown":
            yield paf.Result(pa.py_buffer(b'Shutdown!'))
            # Shut down on background thread to avoid blocking current
            threading.Thread(target=self._shutdown).start()
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