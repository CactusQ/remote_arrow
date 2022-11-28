import os
import ast

import pyarrow as pa
import pyarrow.flight as paf
import pyarrow.parquet as pq
import pyarrow.csv as csv


class RemoteDataset:
    DEFAULT_LOCALHOST = "localhost:5005"
    DELIMITER = "$"

    def __init__(self, filepath=None, hostname=DEFAULT_LOCALHOST):
        self.host, self.port = hostname.split(':')
        self.client = paf.FlightClient(f"grpc+tcp://{self.host}:{self.port}")
        self.descriptor = None
        self.table = None
        self.filename = None
        self.id = None

        if not filepath:
            print(f"No file path given. Connect to one of the available flights:")
            self.list_flights()

        else:
            filename, fileext = os.path.basename(filepath).split('.')

            # Parse file to ArrowTable and upload to server
            if fileext == 'csv':
                self.table = csv.read_csv(filepath)
            elif fileext == 'parquet':
                self.table = pq.read_table(filepath)
            if self.table:
                self.filename = filename
                self.descriptor = paf.FlightDescriptor.for_path(filename)
                writer, _ = self.client.do_put(self.descriptor, self.table.schema)
                writer.write_table(self.table)
                writer.close()
                print(f"Created RemoteDataset \'{self.filename}\' on server: {hostname}")
                print('Total rows: ', len(self.table))

                for id, flight in enumerate(self.client.list_flights()):
                    if self.descriptor == flight.descriptor:
                        self.id = id

                # Override local function calls with remote calls        
                self.override_functions()

            else:
                print(f"Could not create RemoteDataset on server: {hostname}")
                print("Unknown file format: ", fileext)
                print("Use .csv or .parquet")

    def connect(self, flight_id):
        for id, flight in enumerate(self.client.list_flights()):
            if id == flight_id:
                self.descriptor = flight.descriptor
                print("Successfully connected to flight ", id)
                self.id = id
                self.table = self.table
                self.override_functions()
                return
        print(f"ERROR: flight_id {flight_id} not found.")
        return

    # Fetch (doGet) PyArrow Table of connected flight
    def fetch(self):
        if self.table:
            return self.table
        return self.get_flight(self.descriptor)


    # Execute doGet for a given descriptor and return PyArrow Table
    def get_flight(self, descriptor):
        info = self.client.get_flight_info(descriptor)
        for endpoint in info.endpoints:
            for location in endpoint.locations:
                get_client = paf.FlightClient(location)
                reader = get_client.do_get(endpoint.ticket)
                if descriptor.descriptor_type != paf.DescriptorType.UNKNOWN:
                    return reader.read_all()
        
        
    def list_flights(self): 
        print('\n===== Flights======')
        print("[ID]\t[TYPE]\t[DESCRIPTION]")

        for id, flight in enumerate(self.client.list_flights()):
            descriptor = flight.descriptor
            if descriptor.descriptor_type == paf.DescriptorType.PATH:
                print(f"{id}\tFILE\t", RemoteDataset.descriptor_to_readable(descriptor))
            elif descriptor.descriptor_type == paf.DescriptorType.CMD:
                print(f"{id}\tCMD\t", RemoteDataset.descriptor_to_readable(descriptor))
            else:
                print(f"{id} \tN/A\t Flight has been deleted")
        print("")

    # Read metadata of a flight (number of rows, bytes, schema)
    def inspect(self, target_id):
        target_flight = None
        print("Inspecting Flight ", target_id)
        for id, flight in enumerate(self.client.list_flights()):
            if id == target_id:
                descriptor = flight.descriptor
                if descriptor.descriptor_type == paf.DescriptorType.UNKNOWN:
                    print("Flight {target_id} has been deleted.")
                    break
                if descriptor.descriptor_type == paf.DescriptorType.PATH:
                    print("File:", str(descriptor.path[0].decode("utf-8")))
                elif descriptor.descriptor_type == paf.DescriptorType.CMD:
                    print("Command:", descriptor.command)
                target_flight = flight
                break
            
        if target_flight:
            print("Total rows:", end=" ")
            if target_flight.total_records >= 0:
                print(target_flight.total_records)
            else:
                print("Unknown")

            print("Total bytes:", end=" ")
            if target_flight.total_bytes >= 0:
                print(target_flight.total_bytes)
            else:
                print("Unknown")

            print("Number of endpoints:", len(target_flight.endpoints))
            print("Schema:")
            print(target_flight.schema)
            print('---')
        else:
            print(f"ERROR: target_flight {target_id} not found.")

    # Send a do action command of given type and body
    def send_action(self, type, buf_str=None):
        try:
            buf = pa.allocate_buffer(0)
            if buf_str:
                buf = buf_str.encode("utf-8")
            action = pa.flight.Action(type, buf)
            print('Running action', type)
            for result in self.client.do_action(action):
                print("Got result: ", result.body.to_pybytes().decode("utf-8"))
        except pa.lib.ArrowIOError as e:
            print("Error calling action:", e)
    
    def list_actions(self):
        print('\nActions\n=======')
        for action in self.client.list_actions():
            print("Type:", action.type)
            print("Description:", action.description)
            print('---')

    # Override local ArrowTable methods to their RPC version
    def override_functions(self):
        for attr_name in dir(pa.Table):
            attr = getattr(pa.Table, attr_name)
            if callable(attr) and not attr_name.startswith("_"):
                super().__setattr__(attr_name, self.create_rpc_call(attr_name))

    # Transforms a given ArrowTable method to an equivalent remote procedure call
    def create_rpc_call(self, attr_name):
        def rpc(*args, **kwarg):
            args_dict = {i: args[i] for i in range(len(args))}
            cmd_string = self.DELIMITER.join([str(self.id), attr_name, str(args_dict), str(kwarg)])
            descriptor = paf.FlightDescriptor.for_command(cmd_string)
            return self.get_flight(descriptor)
        return rpc

    @classmethod
    def descriptor_to_readable(self, descriptor):
        if descriptor.descriptor_type == paf.DescriptorType.PATH:
            return descriptor.path[0].decode("utf-8")
        elif descriptor.descriptor_type == paf.DescriptorType.CMD:
            decoded_str = descriptor.command.decode("utf-8")

            id, method, strargs, strkwargs = decoded_str.split(RemoteDataset.DELIMITER)
            args = tuple(ast.literal_eval(strargs).values())

            # Parse cmd_string to human readable function call
            result = f"<Flight {id}>.{method}("
            for i in range(len(args)):
                result += str(args[i])+", "

            kwargs = ast.literal_eval(strkwargs)
            for k, v in kwargs.items():
                result += str(k)+"="+str(v)+", "
            if result[-2:] == ", ":
                result = result[:-2]
            result += ")"
            return result
        else:
            return None
