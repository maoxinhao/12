import socket
import sys
import math
from typing import List, Dict, Tuple

class CAScheduler:
    def __init__(self):
        self.servers = []
        self.jobs_scheduled = 0
        
    def connect_to_server(self, port=50000):
        # 握手协议实现
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.connect(('localhost', port))
        
        # 握手
        self.send("HELO")
        response = self.receive()
        if response != "OK":
            raise Exception("Handshake failed")
            
        self.send("AUTH comp8110")
        response = self.receive()
        if response != "OK":
            raise Exception("Authentication failed")
    
    def send(self, message: str):
        self.sock.send(f"{message}\n".encode())
    
    def receive(self) -> str:
        return self.sock.recv(4096).decode().strip()
    
    def get_servers(self):
        self.send("GETS All")
        response = self.receive()
        if response.startswith("DATA"):
            num_servers = int(response.split()[1])
            self.send("OK")
            
            servers_data = []
            for _ in range(num_servers):
                server_info = self.receive()
                parts = server_info.split()
                server = {
                    'type': parts[0],
                    'id': int(parts[1]),
                    'state': parts[2],
                    'cur_avail_time': int(parts[3]),
                    'cores': int(parts[4]),
                    'memory': int(parts[5]),
                    'disk': int(parts[6]),
                    'avail_cores': int(parts[7]),
                    'avail_memory': int(parts[8]),
                    'avail_disk': int(parts[9])
                }
                servers_data.append(server)
            
            self.send("OK")
            response = self.receive()  # Should be "."
            self.servers = servers_data
            return servers_data
    
    def calculate_server_score(self, server: Dict, job_cores: int, job_memory: int, job_disk: int) -> float:
        """计算服务器综合评分"""
        
        # 检查资源是否足够
        if (server['avail_cores'] < job_cores or 
            server['avail_memory'] < job_memory or 
            server['avail_disk'] < job_disk):
            return -1
        
        # 计算可用时间得分（越快越好）
        availability_score = 1.0 / (1.0 + server['cur_avail_time'])
        
        # 计算核心利用率得分（偏好中等利用率）
        current_util = 1.0 - (server['avail_cores'] / server['cores'])
        utilization_score = 1.0 - abs(current_util - 0.7)  # 目标70%利用率
        
        # 计算成本效率得分（每核心成本）
        # 假设成本与核心数成正比（简化模型）
        cost_per_core = server['cores']  # 用核心数代理成本
        cost_score = 1.0 / (1.0 + cost_per_core)
        
        # 综合评分（周转时间权重最高）
        total_score = (0.6 * availability_score + 
                      0.25 * utilization_score + 
                      0.15 * cost_score)
        
        return total_score
    
    def schedule_job(self, job_id: int, job_cores: int, job_memory: int, job_disk: int):
        """调度作业到最佳服务器"""
        
        best_server = None
        best_score = -1
        
        for server in self.servers:
            score = self.calculate_server_score(server, job_cores, job_memory, job_disk)
            if score > best_score:
                best_score = score
                best_server = server
        
        if best_server:
            self.send(f"SCHD {job_id} {best_server['type']} {best_server['id']}")
            response = self.receive()
            if response == "OK":
                self.jobs_scheduled += 1
                return True
        
        return False
    
    def run(self):
        try:
            self.connect_to_server()
            
            while True:
                self.send("REDY")
                response = self.receive()
                
                if response == "NONE":
                    break
                elif response.startswith("JOBP"):
                    # 解析作业信息
                    parts = response.split()
                    job_id = int(parts[1])
                    submit_time = int(parts[2])
                    job_cores = int(parts[4])
                    job_memory = int(parts[5])
                    job_disk = int(parts[6])
                    
                    # 获取最新服务器状态
                    self.get_servers()
                    
                    # 调度作业
                    if not self.schedule_job(job_id, job_cores, job_memory, job_disk):
                        print(f"Failed to schedule job {job_id}", file=sys.stderr)
                        
                elif response.startswith("JCPL"):
                    # 作业完成，继续
                    continue
                else:
                    print(f"Unexpected response: {response}", file=sys.stderr)
            
            # 退出
            self.send("QUIT")
            response = self.receive()
            
        except Exception as e:
            print(f"Error: {e}", file=sys.stderr)
        finally:
            self.sock.close()

if __name__ == "__main__":
    scheduler = CAScheduler()
    scheduler.run()