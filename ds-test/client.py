#!/usr/bin/env python3
"""
ARAS (Adaptive Resource-Aware Scheduling) Client
优化平均周转时间的自适应资源感知调度算法
"""

import sys
import socket


class Job:
    def __init__(self, job_id, submit_time, est_run_time, cores, memory, disk):
        self.job_id = job_id
        self.submit_time = submit_time
        self.est_run_time = est_run_time
        self.cores = cores
        self.memory = memory
        self.disk = disk


class ServerInfo:
    def __init__(self, type_name, server_id, state, start_time, cores, memory, disk, waiting_jobs, running_jobs):
        self.type_name = type_name
        self.server_id = server_id
        self.state = state
        self.start_time = start_time
        self.cores = cores
        self.memory = memory
        self.disk = disk
        self.num_waiting_jobs = waiting_jobs
        self.num_running_jobs = running_jobs


class ARASScheduler:
    def __init__(self):
        self.servers = {}
    
    def parse_gets_response(self, response):
        """解析GETS响应"""
        servers = []
        lines = response.strip().split('\n')
        data_started = False
        
        for line in lines:
            line = line.strip()
            if not line:
                continue
            if line == "DATA" or line.startswith("DATA "):
                data_started = True
                continue
            if not data_started:
                continue
            if line == ".":
                break
            
            parts = line.split()
            start_idx = 1 if len(parts) > 0 and parts[0] == "SENT" else 0
            
            if len(parts) >= start_idx + 9:
                try:
                    type_name = parts[start_idx]
                    server_id = int(parts[start_idx + 1])
                    state = parts[start_idx + 2]
                    start_time = int(parts[start_idx + 3])
                    cores = int(parts[start_idx + 4])
                    memory = int(parts[start_idx + 5])
                    disk = int(parts[start_idx + 6])
                    waiting_jobs = int(parts[start_idx + 7])
                    running_jobs = int(parts[start_idx + 8])
                    
                    server = ServerInfo(type_name, server_id, state, start_time, cores, 
                                      memory, disk, waiting_jobs, running_jobs)
                    servers.append(server)
                    self.servers[(type_name, server_id)] = server
                except (ValueError, IndexError):
                    continue
        
        return servers
    
    def can_run_job(self, server, job):
        """检查服务器是否能运行作业"""
        return (server.cores >= job.cores and 
                server.memory >= job.memory and 
                server.disk >= job.disk)
    
    def calculate_priority(self, server, job, current_time):
        """计算服务器优先级分数"""
        if not self.can_run_job(server, job):
            return -1.0
        
        score = 0.0
        
        # 1. 优先已激活的服务器（避免启动延迟）
        if server.state == "active" or server.state == "idle":
            score += 1000.0
            if server.state == "idle":
                score += 50.0
        elif server.state == "booting":
            score += 500.0
        elif server.state == "inactive":
            score += 50.0
        else:
            return -1.0
        
        # 2. 选择等待队列最短的服务器
        score -= server.num_waiting_jobs * 20.0
        
        # 3. 考虑运行中作业数量
        if server.num_running_jobs == 0:
            score += 50.0
        else:
            if server.num_running_jobs <= 2:
                score -= server.num_running_jobs * 5.0
            else:
                score -= server.num_running_jobs * 15.0
        
        # 4. Best-Fit资源匹配
        core_util = job.cores / server.cores if server.cores > 0 else 0
        mem_util = job.memory / server.memory if server.memory > 0 else 0
        disk_util = job.disk / server.disk if server.disk > 0 else 0
        avg_util = (core_util + mem_util + disk_util) / 3.0
        
        remaining_cores = server.cores - job.cores
        remaining_mem = server.memory - job.memory
        remaining_disk = server.disk - job.disk
        total_capacity = server.cores + server.memory + server.disk
        remaining_total = remaining_cores + remaining_mem + remaining_disk
        remaining_ratio = remaining_total / total_capacity if total_capacity > 0 else 1.0
        
        fit_score = avg_util * 0.7 + (1 - remaining_ratio) * 0.3
        score += fit_score * 50.0
        
        return score
    
    def find_best_server(self, job, current_time):
        """找到最佳服务器"""
        best_server = None
        best_score = -1.0
        
        for (type_name, server_id), server in self.servers.items():
            if server.state == "unavailable":
                continue
            
            score = self.calculate_priority(server, job, current_time)
            if score > best_score:
                best_score = score
                best_server = (type_name, server_id)
        
        return best_server
    
    def backfill_candidate(self, job, current_time):
        """Backfilling策略：短作业填充"""
        if job.est_run_time > 600:
            return None
        
        candidates = []
        for (type_name, server_id), server in self.servers.items():
            if (server.state == "active" and 
                server.num_running_jobs > 0 and
                self.can_run_job(server, job)):
                
                core_util = job.cores / server.cores if server.cores > 0 else 0
                mem_util = job.memory / server.memory if server.memory > 0 else 0
                disk_util = job.disk / server.disk if server.disk > 0 else 0
                avg_util = (core_util + mem_util + disk_util) / 3.0
                
                if server.num_running_jobs <= 3:
                    score = avg_util + 10.0
                else:
                    score = avg_util
                
                candidates.append(((type_name, server_id), score))
        
        if candidates:
            candidates.sort(key=lambda x: x[1], reverse=True)
            return candidates[0][0]
        
        return None


def read_line(sock):
    """从socket读取一行"""
    buffer = ""
    while True:
        chunk = sock.recv(1).decode()
        if not chunk:
            return None
        if chunk == '\n':
            return buffer
        buffer += chunk


def read_until_dot(sock):
    """读取直到遇到单独的"."行"""
    lines = []
    while True:
        line = read_line(sock)
        if line is None:
            break
        if line.strip() == ".":
            break
        lines.append(line)
    return '\n'.join(lines)


def communicate(sock, username):
    """与服务器通信"""
    # 握手
    sock.sendall("HELO\n".encode())
    response = read_line(sock)
    if not response or "OK" not in response:
        return
    
    # 认证
    sock.sendall("AUTH {}\n".format(username).encode())
    response = read_line(sock)
    if not response or "OK" not in response:
        return
    
    scheduler = ARASScheduler()
    
    while True:
        # 发送REDY
        sock.sendall("REDY\n".encode())
        response = read_line(sock)
        
        if not response:
            break
        
        response = response.strip()
        
        # 检查结束信号
        if response == "NONE":
            break
        
        # 处理作业提交
        if response.startswith("JOBN") or response.startswith("JOBP"):
            parts = response.split()
            if len(parts) >= 8:
                try:
                    job = Job(
                        int(parts[2]),
                        int(parts[3]),
                        int(parts[4]),
                        int(parts[5]),
                        int(parts[6]),
                        int(parts[7])
                    )
                    
                    # 获取服务器状态
                    sock.sendall("GETS All\n".encode())
                    
                    # 读取DATA头部
                    data_header = read_line(sock)
                    if data_header and data_header.startswith("DATA"):
                        sock.sendall("OK\n".encode())
                    
                    # 读取服务器数据直到"."
                    gets_data = read_until_dot(sock)
                    gets_response = data_header + "\n" + gets_data if data_header else gets_data
                    
                    servers = scheduler.parse_gets_response(gets_response)
                    
                    # ARAS调度策略
                    server = scheduler.backfill_candidate(job, job.submit_time)
                    if not server:
                        server = scheduler.find_best_server(job, job.submit_time)
                    
                    if server:
                        type_name, server_id = server
                        sock.sendall("SCHD {} {} {}\n".format(job.job_id, type_name, server_id).encode())
                        ack = read_line(sock)
        
        # 处理作业完成
        if response.startswith("JCPL"):
            pass
    
    # 发送QUIT
    sock.sendall("QUIT\n".encode())


def main():
    """主函数"""
    username = sys.argv[1] if len(sys.argv) > 1 else "user"
    host = "localhost"
    port = 50000
    
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((host, port))
        communicate(sock, username)
        sock.close()
    except Exception as e:
        print("Error: {}".format(e), file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
