# Socket server in python using select function

import socket, select, struct, zlib, csv, datetime, time

#Function to broadcast chat messages to all connected clients
def broadcast_data (sock, message):
    #Do not send the message to master socket and the client who has send us the message
    for socket in CONNECTION_LIST:
        if socket != server_socket and socket != sock :
            try :
                socket.send(message)
            except :
                # broken socket connection may be, chat client pressed ctrl+c for example
                socket.close()
                CONNECTION_LIST.remove(socket)

def save_csv (parts):
    now = int(time.time())
    dt = datetime.datetime.fromtimestamp(now).strftime('%Y-%m-%d-%H-%M-%S')
    print(dt)
    with open('./data/msg-' + dt + '.csv', 'w', newline='') as csvfile:
        print("open csvfile...")
        writer = csv.writer(csvfile)
        writer.writerow(["uuid", "travel_type", "location_type", "lat", "lon", "altitude",
                        "precision", "dir", "speed", "timestamp", "load_status", "valid"])
        for part in parts:
            for s in part:
                writer.writerow(s.split(','))

if __name__ == "__main__":

    CONNECTION_LIST = []  # list of socket clients
    RECV_BUFFER = 4096  # Advisable to keep it as an exponent of 2
    PORT = 9876
    MAX_RECORDS = 100

    packagesList = []

    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    # this has no effect, why ?
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_socket.bind(("0.0.0.0", PORT))
    server_socket.listen(10)

    # Add server socket to the list of readable connections
    CONNECTION_LIST.append(server_socket)

    print("Chat server started on port " + str(PORT))

    while 1:
        # Get the list sockets which are ready to be read through select
        read_sockets, write_sockets, error_sockets = select.select(CONNECTION_LIST, [], [])

        for sock in read_sockets:

            # New connection
            if sock == server_socket:
                # Handle the case in which there is a new connection recieved through server_socket
                sockfd, addr = server_socket.accept()
                CONNECTION_LIST.append(sockfd)
                print("Client (%s, %s) connected" % addr)

            # Some incoming message from a client
            else:
                # Data recieved from client, process it
                try:
                    # In Windows, sometimes when a TCP program closes abruptly,
                    # a "Connection reset by peer" exception will be thrown
                    # data = sock.recv(RECV_BUFFER)
                    # read first 4 bytes
                    data = sock.recv(4)
                    # calculate the package length
                    packLen = struct.unpack(">I", data)[0]
                    # read package data
                    packData = sock.recv(packLen)
                    # decompress package data to string
                    packStr = zlib.decompress(packData, 15 + 32).decode("utf-8")

                    # split string and filter empty
                    strParts = list(filter(None, packStr.split('\n')))
                    # append to global package list
                    packagesList.append(strParts)
                    print(len(packagesList))

                    if len(packagesList) >= MAX_RECORDS:
                        # save to csv file
                        save_csv(packagesList)
                        packagesList = []

                # client disconnected, so remove from socket list
                except:
                    broadcast_data(sock, "Client (%s, %s) is offline" % addr)
                    print("Client (%s, %s) is offline" % addr)
                    sock.close()
                    if sock in CONNECTION_LIST:
                        CONNECTION_LIST.remove(sock)
                    continue

    server_socket.close()
