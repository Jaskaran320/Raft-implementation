import grpc
import raft_pb2
import raft_pb2_grpc

# change the localhost ports if number of nodes are not 5


class RaftClient:
    def __init__(self, node_addresses):
        self.index = 0
        self.node_addresses = node_addresses
        self.leader_id = "None"
        self.channels = {
            address: grpc.insecure_channel(address) for address in node_addresses
        }
        self.stubs = {
            address: raft_pb2_grpc.RaftNodeStub(channel)
            for address, channel in self.channels.items()
        }

    def send_request(self, request, server_address, flag=0):
        if self.leader_id == "None":
            try:
                response = self.stubs[
                    self.node_addresses[server_address]
                ].ServeClient(raft_pb2.ServeClientArgs(request=request))
                if response.success:
                    self.leader_id = response.leaderID
                    return response.data
                else:
                    while response.leaderID == "None":
                        response = self.stubs[
                            self.node_addresses[server_address]
                        ].ServeClient(raft_pb2.ServeClientArgs(request=request))
                    self.leader_id = response.leaderID
                    response = self.stubs[
                        self.node_addresses[int(self.leader_id)]
                    ].ServeClient(raft_pb2.ServeClientArgs(request=request))
                    return response.data
            except grpc.RpcError as e:
                self.index = (self.index + 1)
                if self.index > 4:
                    return
                self.send_request(request, self.index, flag=1)
        else:
            try:
                if flag:
                    response = self.stubs[
                        self.node_addresses[server_address]
                    ].ServeClient(raft_pb2.ServeClientArgs(request=request))
                else:
                    response = self.stubs[
                        self.node_addresses[int(self.leader_id)]
                    ].ServeClient(raft_pb2.ServeClientArgs(request=request))

                if response.success:
                    self.leader_id = response.leaderID
                    return response.data
                    
                else:
                    self.leader_id = response.leaderID
                    if self.leader_id != "None":
                        response = self.stubs[
                            self.node_addresses[int(self.leader_id)]
                        ].ServeClient(raft_pb2.ServeClientArgs(request=request))
                        return response.data
            except grpc.RpcError as e:
                self.index = (self.index + 1)
                if self.index > 4:
                    return
                self.send_request(request, self.index, flag=1)

        return ""

    def get(self, key):
        return self.send_request(f"GET {key}", 0)

    def set(self, key, value):
        return self.send_request(f"SET {key} {value}", 0)


if __name__ == "__main__":
    client = RaftClient([
            "localhost:50050",
            "localhost:50051",
            "localhost:50052",
            "localhost:50053",
            "localhost:50054",
        ])
    while True:
        print("1. Get")
        print("2. Set")
        print("3. Exit")
        choice = input("Enter choice: ")
        if choice == "1":
            key = input("Enter key: ")
            print(f"Value: {client.get(key)}")
        elif choice == "2":
            key = input("Enter key: ")
            value = input("Enter value: ")
            print(f"Set: {client.set(key, value)}")
        elif choice == "3":
            break
        else:
            print("Invalid choice")
