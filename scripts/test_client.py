import socket
import struct
import time

MSG_TYPE_NEW_ORDER = 1
SIDE_BUY = 0
ORDER_TYPE_NEW = 0

def send_order(sock, order_id, price, quantity):
    header = struct.pack("<HH", 26, 1)
    payload = struct.pack("<QQIBB", order_id, price, quantity, 0, 0)
    
    sock.sendall(header + payload)
    print(f"Sent Order ID: {order_id}")

if __name__ == "__main__":
    SERVER_IP = "127.0.0.1"
    PORT = 9000
    
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((SERVER_IP, PORT))
        for i in range(1, 6):
            send_order(s, i, 100 + i, 50)
            time.sleep(0.1)