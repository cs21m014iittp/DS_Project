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
mutex = Lock()
buffer = []
buffer_mutex = Lock()
terminate_signal = False

# server specific variables
my_id = int(sys.argv[1])
my_ip = ips[my_id]
primary_id = int(sys.argv[2])
max_nodes = int(sys.argv[3])
update_prob = float(sys.argv[4])

# 3 data items in the datastore
data = []
for i in range(3):
	data.append(0)

#log file related variables	
file_name = f'log_{my_id}.txt'
file_handler = open(file_name , 'w')
file_mutex = Lock()

file_handler.write("\nid: {}".format(my_id))
file_handler.write("\nIP: {}".format(my_ip))
file_handler.write("\nPrimary: {}".format(primary_id))
file_handler.write("\nlog file: {}".format(file_name))


#setup socket to listen for update msgs
sock = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
sock.bind((my_ip,msg_port_no))
sock.listen()

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

# function to send update msg to primary
def send(msg):
	global my_id,primary_id,ips,msg_port_no

	client_socket = socket.socket(socket.AF_INET,socket.SOCK_STREAM)

	#only send to primary
	while True:
		try:
			client_socket.connect((ips[primary_id],msg_port_no))
		except:
			time.sleep(2)
			continue
		client_socket.send(f'{msg}'.encode('ascii'))
		client_socket.close()

		if msg == "Terminate":			
			file_mutex.acquire()
			file_handler.write("\nsending termination request to primary")
			file_mutex.release()
			break

		file_mutex.acquire()
		file_handler.write("\nsending the update \"{}\" to primary".format(msg))
		file_mutex.release()
		break

#function to send the ack of an update to primary
def send_ack(seq):

	global my_id,primary_id,ips,ack_port_no,file_handler,file_mutex
	ack_sock = socket.socket(socket.AF_INET,socket.SOCK_STREAM)

	#send ack to primary
	while True:
		try:
			ack_sock.connect((ips[primary_id],ack_port_no))
		except:
			time.sleep(2)
			continue

		file_mutex.acquire()
		file_handler.write("\nack sent to primary for seq num {}".format(seq))
		file_mutex.release()

		delayAgent()
		ack_sock.send(f"update ack|{my_id}|{seq}".encode('ascii'))
		ack_sock.close()
		break

#function to recieve ack from primary when the node is the one who initiated an update
def ack_recieve():

	global my_ip,ack_port_no,primary_id,file_mutex,file_handler

	#setup socket to listen for ack
	ack_sock = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
	
	while True:
		try:
			ack_sock.bind((my_ip,ack_port_no))
		except:
			time.sleep(5)
			continue

		ack_sock.listen()
		break
	
	while True:

		try:
			conn , addr = ack_sock.accept()
			msg = conn.recv(1024).decode()
		except:
			continue

		conn.close()
		delayAgent()

		file_mutex.acquire()
		file_handler.write("\nupdated")
		file_mutex.release()
		break
		
	ack_sock.close()

# TODO
#total ordering and buffer to be used

#function to carry out an update
def executeUpdate(msg):

	global data,sequence,mutex
	msg_parts = msg.split('|')

	data_index = int(msg_parts[1])
	new_value = int(msg_parts[2])
	master_seq = int(msg_parts[3])
	

	mutex.acquire()	
	sequence = sequence + 1
	old_value= data[data_index] 
	data[data_index] = new_value
	mutex.release()

	file_mutex.acquire()
	file_handler.write("\nexecuting update => old value: {} to new value: {}".format(old_value,new_value))

	mutex.acquire()
	file_handler.write("\ncurrent view of datastore => {}".format(data))
	mutex.release()
	file_mutex.release()

	send_ack(master_seq)

#function to check the total ordering
def checkOrder(msg):
	global sequence,mutex

	master_seq = int(msg.split('|')[3])
	validity = False

	mutex.acquire()
	if master_seq == sequence + 1:
		file_mutex.acquire()
		file_handler.write("\n{} is valid".format(msg))
		file_mutex.release()
		validity=True 
	mutex.release()
	
	return validity

#function to check if the buffer has any satisfying update and execute it
#remove the update from the buffer
def checkBuffer():

	global buffer,buffer_mutex

	flag = False
	buffer_mutex.acquire()

	if len(buffer)>0:
		for msg in buffer:
			if checkOrder(msg):
				executeUpdate(msg)
				buffer.remove(msg)
				flag = True
				break
	buffer_mutex.release()
	return flag


def delivery_handler(msg):
	global buffer,buffer_mutex

	delayAgent()

	msg_parts = msg.split('|')

	data_index = int(msg_parts[1])
	master_seq = int(msg_parts[3])
	
	file_mutex.acquire()
	file_handler.write("\nrecieved update for data item {}, with seq no. {}".format(data_index,master_seq))
	file_mutex.release()

	if checkOrder(msg): #if msg in correct order
		executeUpdate(msg)
		#execute any updates that are now in valid order
		while True:
			if not checkBuffer():
				break

	else:  #msg not in correct order
		valid_updates = False
		curr_executed = False
		
		buffer_mutex.acquire()
		if len(buffer) == 0: #nothing in the buffer then just add to buffer
			buffer.append(msg)
			buffer_mutex.release()
			file_mutex.acquire()
			file_handler.write("\nBuffering the update: {}".format(msg))
			file_mutex.release()

		else: #there are pending updates in the buffer
			buffer_mutex.release()
			while True:
				valid_updates = checkBuffer() # check for any valid update in the buffer 

				if valid_updates: #if there was one valid update
					if not curr_executed and checkOrder(msg): #check validity of current message again if not executed already
						executeUpdate(msg)
						curr_executed=True #mark that the current update has been executed

				if not valid_updates: #no more valid updates in the buffer
					break

			if not curr_executed: #after executing all valid updates, current one is still out of order
				file_mutex.acquire()
				file_handler.write("\nBuffering the update: {}".format(msg))
				file_mutex.release()
				
				buffer_mutex.acquire()
				buffer.append(msg)
				buffer_mutex.release()

#keep listening for update msgs from the primary 
def msg_recieve():
	global sock,terminate_signal


	while True:
		time.sleep(2)
		conn , addr = sock.accept()
		msg = conn.recv(1024).decode()

		#print(msg)
		if msg == 'Terminate signal':
			terminate_signal = True
			print("terminate_signal: " ,terminate_signal)
			conn.close()
			break
	
		update_thread = Thread(target= delivery_handler, args = (msg,))
		update_thread.start()
	
		conn.close()
	sock.close()



# launch a thread that will continuously listen
recv_thread = Thread(target= msg_recieve )
recv_thread.setDaemon(True)
recv_thread.start()


start = input("press enter to start the server:")
time.sleep(5)

for i in range(20):

	print("round {}:".format(i))
	time.sleep(int(random.random()*5))
	
	#after a random amount of time randomly read the data
	file_mutex.acquire()
	mutex.acquire()
	file_handler.write("\nread current data => {}".format(data))
	mutex.release()
	file_mutex.release()
		
	if random.random() <= update_prob:
		data_index = int(random.random()*100)%3
		new_value = int(random.random()*50)
		msg = f'{my_id}|{data_index}|{new_value}'
	
		file_mutex.acquire()
		file_handler.write("\nInitiating update for data item {} ,to new value {}".format(data_index,new_value))
		file_mutex.release()

		#send to primary
		send(msg)

		#wait for ack from primary
		ack_recieve()

#terminate_msg = f'{}'
print("sending terminate signal")
send("Terminate")

while True:
	if terminate_signal:
		file_mutex.acquire()
		file_handler.write("\nstopping server")
		file_mutex.release()
		break
	

print("done")
sock.close()
file_handler.close()
