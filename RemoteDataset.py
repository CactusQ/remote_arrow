from fileinput import filename
import os

import pyarrow as pa
import pyarrow.flight as paf
import pyarrow.parquet as pq
import pyarrow.csv as csv


class RemoteDataset:
    DEFAULT_LOCALHOST = "localhost:5005"
    DELIMITER = "$$$"

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

                # Get id 
                self.id = len(list(self.client.list_flights()))-1
                # Override local function calls with remote calls        
                self.override_functions()
                print(f"Created RemoteDataset \'{self.filename}\' (id: {self.id}) on server: {hostname}")
                print('Total rows: ', len(self.table))

            else:
                print(f"Could not create RemoteDataset on server: {hostname}")
                print("Unknown file format: ", fileext)
                print("Use .csv or .parquet")

    def connect(self, flight_id):
        for id, flight in enumerate(self.client.list_flights()):
            if id == flight_id:
                self.descriptor = flight.descriptor
                self.id = flight_id
                print("Successfully connected to flight: ", self.descriptor)

                # Override local function calls with remote calls        
                self.override_functions()
                return
        print(f"ERROR: flight_id {id} not found.")
        return
        
    def list_flights(self):
        print('Flights\n=======')
        for id, flight in enumerate(self.client.list_flights()):
            print("Flight_ID: ", id)
            descriptor = flight.descriptor
            if descriptor.descriptor_type == paf.DescriptorType.PATH:
                print("Path:", descriptor.path)
            elif descriptor.descriptor_type == paf.DescriptorType.CMD:
                print("Command:", descriptor.command)
            else:
                print("Unknown descriptor type")

            print("Total records:", end=" ")
            if flight.total_records >= 0:
                print(flight.total_records)
            else:
                print("Unknown")

            print("Total bytes:", end=" ")
            if flight.total_bytes >= 0:
                print(flight.total_bytes)
            else:
                print("Unknown")
            print("Number of endpoints:", len(flight.endpoints))
            print("Schema:")
            print(flight.schema)
            print('---')
    
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
            info = self.client.get_flight_info(descriptor)
            for endpoint in info.endpoints:
                for location in endpoint.locations:
                    get_client = paf.FlightClient(location)
                    reader = get_client.do_get(endpoint.ticket)
                    if descriptor.descriptor_type == paf.DescriptorType.CMD:
                        return reader.read_all()

        return rpc