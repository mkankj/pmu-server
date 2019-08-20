
import threading
import queue
import socket
import struct
import time
from collections import deque

tr_ctrl_list = []  # 线程控制列表,用于添加为每个socket连接创建线程类的实例

DMDQUE_MAX_LEN = 11
DMDQUE_MIDDLE  = int(DMDQUE_MAX_LEN/2)

'''
数据处理线程
'''
class data_deal_thread(threading.Thread):
	
	def __init__(self, sk_hd, address):
		threading.Thread.__init__(self)
		self.sk_hd = sk_hd
		self.__address = address
		self.data_match_queue = deque([], DMDQUE_MAX_LEN)  # 数据匹配队列,最大容量为11
		self.target_num = 0
		self.local_num = 0
		self.property  = 0

	def run(self):
		counter = 0
		
		while True:
			time.sleep(1)
			counter += 1  # 节拍计数  1s一次
			
			'''数据监听、接收、解包和推入队列'''
			try:
				msg = self.sk_hd.recv(512)
				if not msg:  # 如果收到为空 表明连接可能断开了  线程退出处理
					self.__before_exit()
					print("线程断开退出:", self.name)
					break

				if len(msg) == 148:
					upack_data_tup = struct.unpack('I2H6B3H31fI', msg)
					self.data_match_queue.append(upack_data_tup)
					self.target_num = upack_data_tup[1]
					self.property   = upack_data_tup[2]
					self.local_num  = upack_data_tup[10]

			except BlockingIOError:
				pass

			'''数据匹配和计算,5s一次'''
			if counter % 5 == 0:
				self.match_send()

			'''无数据超时检测,超时后断开连接'''
			if counter == 60*10:  
				counter = 0
				if self.target_num == 0:
					self.__before_exit()
					print("线程超时退出:", self.name)
					break

	'''匹配和计算函数'''			
	def match_send(self):
		if self.target_num == 0 or self.local_num == 0:
			print("当前无数据",self.name)
			return

		if len(self.data_match_queue) < DMDQUE_MAX_LEN:   #自己的数据匹配队列少于11个 返回不匹配
			print("队列不满",self.name)
			return
			
		for x in tr_ctrl_list:  # 从列表头开始匹配
			if x == self:  # 跳过自己
				continue
			
			if x.local_num == self.target_num:
				print(self.name,"匹配成功",x.name)
				
				if len(x.data_match_queue) < DMDQUE_MAX_LEN:  #匹配到的设备的数据匹配队列少于11个 返回
					print("目标队列不满",self.name,"目标",x.name)
					return
				
				for y in x.data_match_queue:
					if y[3:9] == self.data_match_queue[DMDQUE_MIDDLE][3:9]:
						
						if self.property == 1:
							if x.property == 2:
								send_msg = self.data_deal_pack(self.data_match_queue[DMDQUE_MIDDLE][3:43], y[3:43])
								self.sk_hd.send(send_msg)
								print(send_msg.hex())
								print("主机匹配成功",self.name)
								return
							else:
								print("属性错误1",self.name)
								return

						if self.property == 2:
							if x.property == 1:
								send_msg = self.data_deal_pack(y[3:43], self.data_match_queue[DMDQUE_MIDDLE][3:43])
								self.sk_hd.send(send_msg)
								print("主机匹配成功",self.name)
								return
							else:
								print("属性错误2",self.name)
								return
						else:
							print("属性错误3",self.name)
							return

				print("目标队列时间戳无匹配",self.name)
				return

		print("无匹配目标",self.name)
		return 


	def data_deal_pack(self, a, b):
		
		retup = [2018915346, ]
		retup = retup + list(a)
		retup = retup + list(b)

		retup.append( (a[10] - b[10])/a[10] * 100 )  #Ua
		retup.append( (a[11] - b[11])/a[11] * 100 )  #Ia
		retup.append( (a[12] - b[12])/a[12] * 100 )  #Ub
		retup.append( (a[13] - b[13])/a[13] * 100 )  #Ib
		retup.append( (a[14] - b[14])/a[14] * 100 )  #Uc
		retup.append( (a[15] - b[15])/a[15] * 100 )  #Ic
		
		retup.append( (a[34] - b[34])/a[34] * 60 )   #UaAng
		retup.append( (a[37] - b[37])/a[37] * 60 )   #IaAng
		retup.append( (a[35] - b[35])/a[35] * 60 )   #UbAng
		retup.append( (a[38] - b[38])/a[38] * 60 )   #IbAng
		retup.append( (a[36] - b[36])/a[36] * 60 )   #UcAng
		retup.append( (a[39] - b[39])/a[39] * 60 )   #IcAng
		
		retup.append( (a[18] - b[18])/a[18] * 100 )  #Pa
		retup.append( (a[19] - b[19])/a[19] * 100 )  #Pb
		retup.append( (a[20] - b[20])/a[20] * 100 )  #Pc
		retup.append( (a[21] - b[21])/a[21] * 100 )  #Ptot

		retup.append( (a[22] - b[22])/a[22] * 100 )  #Qa
		retup.append( (a[23] - b[23])/a[23] * 100 )  #Qb
		retup.append( (a[24] - b[24])/a[24] * 100 )  #Qc
		retup.append( (a[25] - b[25])/a[25] * 100 )  #Qtot

		retup.append( (a[26] - b[26])/a[26] * 100 )  #Sa
		retup.append( (a[27] - b[27])/a[27] * 100 )  #Sb
		retup.append( (a[28] - b[28])/a[28] * 100 )  #Sc
		retup.append( (a[29] - b[29])/a[29] * 100 )  #Stot

		retup.append(4041128970)

		return struct.pack('<I6B3H31f6B3H55fI', *retup)
		

	'''线程退出前的处理函数'''
	def __before_exit(self):
		print("断开的客户端", self.__address)
		self.data_match_queue.clear()
		tr_ctrl_list.remove(self)
		self.sk_hd.close()
		print("当前线程控制列表长度:", len(tr_ctrl_list))
		print("当前线程数量:", threading.activeCount())


'''
主线程
初始化socket
监听端口连接
为每个连接开辟数据处理线程(data_deal_thread)实例
'''
sk = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sk.setblocking(False)
sk.bind((socket.gethostname(), 33776))
sk.listen(30)
print("服务器启动，监听客户端链接")

while True:
	try:
		sk_hd, address = sk.accept()  # 监听端口
		
		tr_ctrl_list.append(data_deal_thread(sk_hd, address))  # 创建线程并添加至线程控制列表末尾
		print("连接地址:", str(address))
		
		tr_ctrl_list[-1].start()  # 运行刚添加的线程
		
		print("当前线程控制列表长度:", len(tr_ctrl_list))
		print("当前线程数量:", threading.activeCount())
		
	except BlockingIOError:
		time.sleep(5)
		pass
