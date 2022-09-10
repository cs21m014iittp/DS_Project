'''
port 8080 - to listen for update msg
port 8081 - to listen for ack 

updating client:
sends update to 8080
waits for ack on 8081

non-updating clients:
listens for msg from primary on 8080
send ack to primary on 8081

primary:
listens for update msg on 8080
sends update msg to other clients on 8080
listens for ack on 8081
sends ack to updating client on 8081
'''

import socket
import sys
from threading import Thread,Lock
import time
import random

#design-related variables
ips = ["10.0.0.1","10.0.0.2","10.0.0.3","10.0.0.4","10.0.0.5","10.0.0.6","10.0.0.7","10.0.0.8","10.0.0.9","10.0.0.10"]
msg_port_no = 8080
ack_port_no = 8081
sequence = 0
data_mutex = Lock()
ack_mutex = Lock()
send_mutex = Lock()
ack_list = []
terminate_req = 0
terminate_signal = False

# server specific variables
my_id = int(sys.argv[1])
max_nodes = int(sys.argv[2])
my_ip = ips[my_id]

# replicated data is an array of numbers
data = []
for i in range(3):
	data.append(0)

#log file related variables
file_name = f'log_{my_id}.txt'
file_handler = open(file_name , 'w')
file_mutex = Lock()

file_handler.write("\nid: {}".format(my_id))
file_handler.write("\nIP: {}".format(my_ip))
file_handler.write("\nlog file: {}".format(file_name))

#setup socket to listen for update msgs
sock = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
sock.bind((my_ip,msg_port_no))
sock.listen()

#setup socket to listen for ack
ack_sock = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
ack_sock.bind((my_ip,ack_port_no))
ack_sock.listen()

#setup variables to be used by delay agent
time.sleep(1)
random.seed()
delay_counter = 0
delay_counter_mutex = Lock()

#function to introduce artifical network delays
# def delayAgent():
# 	global delay_counter,delay_counter_mutex

# 	delay_counter_mutex.acquire()
# 	delay_counter = delay_counter + 1
# 	delay_counter_mutex.release()

# 	if delay_counter%2==0:
# 		duration = int(random.random()*5)+1
# 	else:
# 		duration = int(random.random()*5)+10

# 	time.sleep(duration)

def delayAgent():

	chance = int(random.random()*1000)

	if chance % 3 == 0:
		duration = random.randint(1,5)
	elif chance % 3 == 1:
		duration = random.randint(10,20)
	else:
		duration = random.randint(30,40)
	time.sleep(duration)

#function to send update msgs to all the replicas
def send(msg):
	global my_id,max_nodes,ips,msg_port_no

	errored_client=[]  #list of clients for which connection failed

	for i in range(0,max_nodes):
		if i==my_id:
			continue

		client_socket = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
		try:
			client_socket.connect((ips[i],msg_port_no))
		except :
			errored_client.append(i)
			file_mutex.acquire()
			file_handler.write("\nconnection to client {} failed...".format(i))
			file_mutex.release()
			continue

		client_socket.send(f'{msg}'.encode('ascii'))
		
		file_mutex.acquire()
		file_handler.write("\nsent update msg : \"{}\" to client: {}".format(msg,i))
		file_mutex.release()
		
		client_socket.close()

	#keep trying to send to errored clients
	while len(errored_client)>0:
		for i in errored_client:
			client_socket = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
			try:
				client_socket.connect((ips[i],msg_port_no))
			except :
				continue

			client_socket.send(f'{msg}'.encode('ascii'))
			
			file_mutex.acquire()
			file_handler.write("\nsent update msg : {} to client: {}".format(msg,i))
			file_mutex.release()
		
			client_socket.close()
			errored_client.remove(i)

#function to send ack to the client that initiated update
def send_ack(client_id):

	global ips,ack_port_no,file_handler,file_mutex

	client_socket = socket.socket(socket.AF_INET,socket.SOCK_STREAM)

	while True:
		try:
			client_socket.connect((ips[client_id],ack_port_no))
		except:
			continue

		client_socket.send("update ack".encode('ascii'))  #delay of this is implemented at reciever side
		
		file_mutex.acquire()
		file_handler.write("\nack sent to client: {}".format(client_id))
		file_mutex.release()

		client_socket.close()
		break


def ack_recieve():
	global ack_mutex,ack_list,ack_sock,file_handler,file_mutex

	while True:

		conn , addr = ack_sock.accept()
		msg = conn.recv(1024).decode()
		conn.close()

		ack_seq = int(msg.split('|')[-1])

		file_mutex.acquire()
		file_handler.write("\nrecieved ack from {} for seq = {}".format(msg.split('|')[1],ack_seq))
		file_mutex.release()

		ack_mutex.acquire()
		ack_list.append(ack_seq)
		local_ack_list = ack_list
		ack_mutex.release()

		file_mutex.acquire()
		file_handler.write("\nack list: {}".format(local_ack_list))
		file_mutex.release()

	ack_sock.close()


# primary remote-write protocol
def update_protocol(msg):

	global data_mutex,sequence,data,ack_list,ack_mutex, max_nodes

	delayAgent()
	msg_parts = msg.split('|')
	client_id = int(msg_parts[0])
	data_index = int(msg_parts[1])
	new_value = int(msg_parts[2])
	
	data_mutex.acquire()
	#increment sequence
	sequence = sequence + 1
	curr_msg_seq = sequence			#local copy of seq, later used for counting acks
	
	#update local copy
	data[data_index] = new_value

	#append sequence number to msg
	msg = msg + f'|{sequence}'
	data_mutex.release()

	file_mutex.acquire()
	file_handler.write("\nreceived update from {} , giving seq num = {}".format(client_id,curr_msg_seq))
	file_mutex.release()

	# send update to all replicas
	send_mutex.acquire()
	send(msg)  
	send_mutex.release()
	
	# ack phase 

	ack_count = 0 
	while ack_count < max_nodes - 1:
		time.sleep(2)
		
		#wait till ack_list is filled with a ack
		flag =0
		while True:

			ack_mutex.acquire()
			if len(ack_list)>0:
				flag=1
			ack_mutex.release()

			if flag == 1:
				break
			time.sleep(4)

		file_mutex.acquire()
		file_handler.write("\nsearching ack list for seq num: {}".format(curr_msg_seq))
		file_mutex.release()
		
		ack_mutex.acquire()
		ack_count = ack_count + ack_list.count(curr_msg_seq)  #count the number of acks for my_seq
		ack_list = list(filter(lambda x: x!=curr_msg_seq , ack_list)) #remove all the acks for my_seq
		local_ack_list = ack_list
		ack_mutex.release()

		file_mutex.acquire()
		file_handler.write("\nack list after filtering: {}".format(local_ack_list))
		file_handler.write("\nack_count for seq num {}: {}".format(curr_msg_seq,ack_count))
		file_mutex.release()

	send_ack(client_id)


#function to handle incoming update msgs
def msg_recieve():
	global sock,terminate_req,terminate_signal

	while True:
		conn , addr = sock.accept()
		msg = conn.recv(1024).decode()
		
		print(msg)
		if msg == 'Terminate':
			terminate_req = terminate_req + 1
			print("number of terminate req = ",terminate_req)

		else:		
			update_thread = Thread(target= update_protocol, args = (msg,))
			update_thread.start()

		if terminate_req == max_nodes - 1:
			print("terminating all servers")
			send("Terminate signal")
			terminate_signal = True
			break
		#peer_name = chr((int(addr[0].split('.')[-1])+64))
		#file_handler.write(peer_name," says: ",msg)
		conn.close()
	sock.close()



# launch a thread that will continuously listen for update msgs
recv_thread = Thread(target= msg_recieve )
recv_thread.setDaemon(True)
recv_thread.start()

# launch a thread that will continuously listen for acks
ack_thread = Thread(target= ack_recieve )
ack_thread.setDaemon(True)
ack_thread.start()

while True:

	time.sleep(int(random.random()*20))
	
	#after a random amount of time randomly read the data
	file_mutex.acquire()
	data_mutex.acquire()
	file_handler.write("\nread current data: {}".format(data))
	data_mutex.release()
	file_mutex.release()

	if terminate_signal:
		print("server stopped....")
		file_mutex.acquire()
		file_handler.write("\nterminating server")
		file_mutex.release()
		break



sock.close()
file_handler.close()