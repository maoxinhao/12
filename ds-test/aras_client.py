#!/usr/bin/env python3
"""
ARAS (Adaptive Resource-Aware Scheduling) Client Implementation
优化平均周转时间的自适应资源感知调度算法
"""

import sys
import socket
import re
import time

# 服务器状态常量（不使用enum以兼容Python 3.6）
SERVER_STATE_INACTIVE = "inactive"
SERVER_STATE_BOOTING = "booting"
SERVER_STATE_IDLE = "idle"
SERVER_STATE_ACTIVE = "active"
SERVER_STATE_UNAVAILABLE = "unavailable"


class ServerResource(object):
    """服务器资源"""
    def __init__(self, cores=0, memory=0, disk=0):
        self.cores = cores
        self.memory = memory
        self.disk = disk


class Job(object):
    """作业信息"""
    def __init__(self, job_id=0, submit_time=0, est_run_time=0, cores=0, memory=0, disk=0):
        self.job_id = job_id
        self.submit_time = submit_time
        self.est_run_time = est_run_time
        self.cores = cores
        self.memory = memory
        self.disk = disk


class ServerInfo(object):
    """服务器信息"""
    def __init__(self, type_name="", server_id=0, state=SERVER_STATE_INACTIVE, start_time=0,
                 cores=0, memory=0, disk=0, num_waiting_jobs=0, num_running_jobs=0,
                 bootup_time=0, hourly_rate=0.0):
        self.type_name = type_name
        self.server_id = server_id
        self.state = state
        self.start_time = start_time
        self.cores = cores
        self.memory = memory
        self.disk = disk
        self.num_waiting_jobs = num_waiting_jobs
        self.num_running_jobs = num_running_jobs
        self.bootup_time = bootup_time
        self.hourly_rate = hourly_rate


class ARASScheduler:
    """ARAS调度器"""
    
    def __init__(self):
        self.servers = {}  # Dict[(type_name, server_id), ServerInfo]
        self.jobs = {}  # Dict[job_id, Job]
        self.current_time = 0
        
    def parse_gets_response(self, response):
        """解析GETS命令的响应"""
        servers = []
        lines = response.strip().split('\n')
        
        # 查找"DATA"标记
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
            
            # 跳过可能的时间戳和"SENT"标记
            # 格式可能是: "SENT type id state start_time cores memory disk waiting_jobs running_jobs"
            # 或者直接: "type id state start_time cores memory disk waiting_jobs running_jobs"
            parts = line.split()
            # 如果第一个部分是"SENT"，跳过它
            start_idx = 1 if len(parts) > 0 and parts[0] == "SENT" else 0
            
            if len(parts) >= start_idx + 9:
                try:
                    type_name = parts[start_idx]
                    server_id = int(parts[start_idx + 1])
                    state_str = parts[start_idx + 2]
                    start_time = int(parts[start_idx + 3])
                    cores = int(parts[start_idx + 4])
                    memory = int(parts[start_idx + 5])
                    disk = int(parts[start_idx + 6])
                    waiting_jobs = int(parts[start_idx + 7])
                    running_jobs = int(parts[start_idx + 8])
                    
                    # 确定服务器状态（使用字符串常量）
                    state = state_str  # 直接使用字符串状态
                    
                    server = ServerInfo(
                        type_name=type_name,
                        server_id=server_id,
                        state=state,
                        start_time=start_time,
                        cores=cores,
                        memory=memory,
                        disk=disk,
                        num_waiting_jobs=waiting_jobs,
                        num_running_jobs=running_jobs
                    )
                    servers.append(server)
                    self.servers[(type_name, server_id)] = server
                except (ValueError, IndexError) as e:
                    # 调试：打印无法解析的行
                    continue
        
        return servers
    
    def parse_job(self, msg):
        """解析作业消息（JOBN或JOBP）"""
        parts = msg.split()
        if len(parts) >= 8:
            try:
                job_id = int(parts[2])
                submit_time = int(parts[3])
                est_run_time = int(parts[4])
                cores = int(parts[5])
                memory = int(parts[6])
                disk = int(parts[7])
                
                job = Job(
                    job_id=job_id,
                    submit_time=submit_time,
                    est_run_time=est_run_time,
                    cores=cores,
                    memory=memory,
                    disk=disk
                )
                self.jobs[job_id] = job
                return job
            except (ValueError, IndexError):
                return None
        return None
    
    def can_server_run_job(self, server, job):
        """检查服务器是否可以运行作业"""
        return (server.cores >= job.cores and 
                server.memory >= job.memory and 
                server.disk >= job.disk)
    
    def calculate_resource_fit_score(self, server, job):
        """计算资源匹配分数（Best-Fit策略）"""
        if not self.can_server_run_job(server, job):
            return -1.0
        
        # 计算资源利用率（避免资源浪费）
        core_util = job.cores / server.cores if server.cores > 0 else 0
        mem_util = job.memory / server.memory if server.memory > 0 else 0
        disk_util = job.disk / server.disk if server.disk > 0 else 0
        
        # Best-Fit: 选择资源最匹配的服务器（利用率高但不过度）
        # 使用加权平均，偏向于选择利用率适中的服务器
        avg_util = (core_util + mem_util + disk_util) / 3.0
        
        # 避免资源碎片化：优先选择剩余资源最少的（但仍然足够的）服务器
        remaining_cores = server.cores - job.cores
        remaining_mem = server.memory - job.memory
        remaining_disk = server.disk - job.disk
        
        # 标准化剩余资源（越小越好，但必须足够）
        total_capacity = server.cores + server.memory + server.disk
        remaining_total = remaining_cores + remaining_mem + remaining_disk
        remaining_ratio = remaining_total / total_capacity if total_capacity > 0 else 1.0
        
        # 综合评分：高利用率 + 低剩余资源 = 好匹配
        fit_score = avg_util * 0.7 + (1 - remaining_ratio) * 0.3
        
        return fit_score
    
    def calculate_priority_score(self, server, job, current_time):
        """计算服务器优先级评分"""
        if not self.can_server_run_job(server, job):
            return -1.0
        
        priority = 0.0
        
        # 1. 优先使用已激活的服务器（避免启动延迟）- 最重要的因素
        if server.state == SERVER_STATE_ACTIVE or server.state == SERVER_STATE_IDLE:
            priority += 1000.0  # 已激活的服务器获得非常高优先级
            # 空闲服务器（IDLE）比活跃服务器（ACTIVE）更优先（因为可以立即运行）
            if server.state == SERVER_STATE_IDLE:
                priority += 50.0
        elif server.state == SERVER_STATE_BOOTING:
            # 正在启动的服务器，计算启动完成时间
            # 需要知道启动时间，如果没有，使用默认值
            bootup_time = server.bootup_time if server.bootup_time > 0 else 60
            elapsed = max(0, current_time - server.start_time)
            bootup_remaining = max(0, bootup_time - elapsed)
            # 启动完成时间越短，优先级越高
            if bootup_remaining <= 10:
                priority += 800.0  # 即将完成启动
            elif bootup_remaining <= 30:
                priority += 500.0
            else:
                priority += max(100.0, 500.0 - bootup_remaining * 5.0)
        elif server.state == SERVER_STATE_INACTIVE:
            # 未激活的服务器优先级最低，但仍然可以考虑
            priority += 50.0
        else:
            # UNAVAILABLE服务器不能使用
            return -1.0
        
        # 2. 选择等待队列最短的服务器（减少作业等待时间）
        priority -= server.num_waiting_jobs * 20.0
        
        # 3. 考虑运行中作业数量（避免过载，但允许合理的负载）
        if server.num_running_jobs == 0:
            priority += 50.0  # 空闲服务器优先
        else:
            # 运行中作业越多，优先级越低，但不完全排除
            # 允许一定程度的负载以提高利用率
            if server.num_running_jobs <= 2:
                priority -= server.num_running_jobs * 5.0
            else:
                priority -= server.num_running_jobs * 15.0
        
        # 4. 资源匹配度（Best-Fit）- 避免资源浪费
        fit_score = self.calculate_resource_fit_score(server, job)
        if fit_score > 0:
            priority += fit_score * 50.0
        
        return priority
    
    def find_best_server(self, job, current_time):
        """找到最适合的服务器"""
        best_server = None
        best_score = -1.0
        
        for (type_name, server_id), server in self.servers.items():
            if server.state == SERVER_STATE_UNAVAILABLE:
                continue
            
            score = self.calculate_priority_score(server, job, current_time)
            if score > best_score:
                best_score = score
                best_server = (type_name, server_id)
        
        return best_server
    
    def schedule_job(self, job, current_time):
        """调度作业"""
        # 首先尝试找到最佳匹配的服务器
        best_server = self.find_best_server(job, current_time)
        return best_server
    
    def backfill_candidate(self, job, current_time):
        """Backfilling策略：尝试将小作业填充到已有作业的服务器空闲时段"""
        # 只对相对较短的作业考虑backfilling（提高资源利用率）
        # 使用相对值：如果作业运行时间小于平均值的50%，可以考虑backfilling
        # 这里简化处理：对小于600秒（10分钟）的作业考虑backfilling
        if job.est_run_time > 600:
            return None
        
        # 寻找有运行中作业但有空闲资源的服务器（backfilling的关键）
        candidates = []
        for (type_name, server_id), server in self.servers.items():
            # 只考虑激活的服务器
            if server.state != SERVER_STATE_ACTIVE:
                continue
            
            # 必须有能力运行作业
            if not self.can_server_run_job(server, job):
                continue
            
            # Backfilling策略：即使有运行中的作业，只要资源足够就可以运行
            # 这样可以提高资源利用率，同时不阻塞短作业
            if (server.cores >= job.cores and 
                server.memory >= job.memory and 
                server.disk >= job.disk):
                
                # 计算backfilling分数
                # 优先选择：1) 有运行中作业的（真正backfilling） 2) 资源利用率高的
                if server.num_running_jobs > 0:
                    # 真正的backfilling情况：有运行中作业但资源足够
                    score = self.calculate_resource_fit_score(server, job)
                    # 运行中作业数适中时优先（提高利用率但不阻塞）
                    if server.num_running_jobs <= 3:
                        score += 10.0
                    candidates.append(((type_name, server_id), score))
        
        if candidates:
            # 选择最佳匹配
            candidates.sort(key=lambda x: x[1], reverse=True)
            return candidates[0][0]
        
        return None


def communicate_with_server(sock, username="user", use_newline=False):
    """与服务器通信的主循环"""
    # 服务器总是期望换行符结尾（根据错误信息）
    # 如果服务器使用-n选项，客户端也必须使用换行符
    terminator = "\n"
    buffer = ""
    
    # 握手
    sock.sendall("HELO\n".encode())
    response = sock.recv(1024).decode().strip()
    if "OK" not in response:
        print("Handshake failed: {}".format(response), file=sys.stderr)
        return
    
    # 认证
    sock.sendall("AUTH {}\n".format(username).encode())
    response = sock.recv(1024).decode().strip()
    if "OK" not in response:
        print("Auth failed: {}".format(response), file=sys.stderr)
        return
    
    scheduler = ARASScheduler()
    
    while True:
        # 发送REDY
        sock.sendall("REDY\n".encode())
        
        # 接收响应（按换行符分割，因为服务器总是发送换行符）
        response = ""
        # 从缓冲区中读取一行
        if buffer:
            if '\n' in buffer:
                parts = buffer.split('\n', 1)
                response = parts[0]
                buffer = parts[1] if len(parts) > 1 else ""
            else:
                # 如果缓冲区没有换行符，继续接收
                pass
        
        # 如果缓冲区没有完整消息，接收更多数据
        if not response:
            try:
                sock.settimeout(10.0)  # 10秒超时
                chunk = sock.recv(4096).decode()
                sock.settimeout(None)  # 取消超时
            except socket.timeout:
                print("Timeout waiting for server response", file=sys.stderr)
                break
            if not chunk:
                break
            buffer += chunk
            if '\n' in buffer:
                parts = buffer.split('\n', 1)
                response = parts[0]
                buffer = parts[1] if len(parts) > 1 else ""
            else:
                # 继续等待数据
                continue
        
        if not response:
            break
        
        response = response.strip()
        
        # 检查是否是结束信号
        if "QUIT" in response or "NONE" in response:
            break
        
        # 处理作业提交
        if response.startswith("JOBN") or response.startswith("JOBP"):
            job = scheduler.parse_job(response)
            if not job:
                continue
            
            # 获取当前服务器状态
            sock.sendall("GETS All\n".encode())
            
            # 接收GETS响应 - 格式：先发送"DATA N M"头部，然后是服务器信息，最后是"."
            # 先接收DATA头部并确认
            data_header = ""
            temp_buffer = ""
            # 接收DATA头部（第一行）
            try:
                sock.settimeout(5.0)  # 5秒超时
                while True:
                    chunk = sock.recv(1024).decode()
                    if not chunk:
                        break
                    temp_buffer += chunk
                    if '\n' in temp_buffer:
                        lines = temp_buffer.split('\n')
                        data_header = lines[0].strip()
                        # 保存剩余数据
                        temp_buffer = '\n'.join(lines[1:])
                        break
                sock.settimeout(None)  # 取消超时
            except socket.timeout:
                print("Timeout waiting for DATA header", file=sys.stderr)
                sock.settimeout(None)
            
            # 发送OK确认DATA头部
            if data_header and data_header.startswith("DATA"):
                sock.sendall("OK\n".encode())
            else:
                # 如果没有收到DATA头部，可能响应格式不同
                sock.sendall("OK\n".encode())
            
            # 接收GETS数据（可能很大，以"\n."结束）
            gets_response = temp_buffer if temp_buffer else ""
            max_recv = 100  # 最大接收次数，防止无限循环
            recv_count = 0
            while recv_count < max_recv:
                # 设置socket超时，避免无限等待
                try:
                    sock.settimeout(5.0)  # 5秒超时
                    chunk = sock.recv(16384).decode()
                    sock.settimeout(None)  # 取消超时
                except socket.timeout:
                    # 超时了，检查是否已经收到完整数据
                    if "." in gets_response:
                        break
                    recv_count += 1
                    continue
                
                if not chunk:
                    recv_count += 1
                    if "." in gets_response:
                        break
                    continue
                
                gets_response += chunk
                recv_count = 0  # 重置计数器
                
                # 检查是否收到完整响应（以"\n."或单独的"."结束）
                if "\n." in gets_response:
                    # 找到最后一个"\n."
                    idx = gets_response.rfind("\n.")
                    if idx >= 0:
                        gets_response = gets_response[:idx+2]  # 包括"\n."
                        break
                elif gets_response.rstrip().endswith(".") and len(gets_response.rstrip()) > 1:
                    # 以单独的"."结束（但不是在行首）
                    break
            
            servers = scheduler.parse_gets_response(gets_response)
            scheduler.current_time = job.submit_time
            
            # ARAS调度策略：
            # 1. 首先尝试backfilling（仅对短作业，提高资源利用率）
            server = scheduler.backfill_candidate(job, job.submit_time)
            
            # 2. 如果没有backfilling机会，使用正常调度（Best-Fit + 优先级）
            if not server:
                server = scheduler.schedule_job(job, job.submit_time)
            
            if server:
                type_name, server_id = server
                # 发送调度命令
                schedule_cmd = "SCHD {} {} {}\n".format(job.job_id, type_name, server_id)
                sock.sendall(schedule_cmd.encode())
                
                # 接收确认（按换行符分割）
                ack = ""
                ack_chunk = sock.recv(1024).decode()
                if ack_chunk:
                    if '\n' in ack_chunk:
                        ack = ack_chunk.split('\n')[0].strip()
                    else:
                        ack = ack_chunk.strip()
                
                # 验证调度是否成功
                if ack and "OK" not in ack and "ERR" in ack:
                    print("Scheduling failed for job {}: {}".format(job.job_id, ack), file=sys.stderr)
            else:
                # 如果没有可用服务器，这不应该发生（系统应该总是有服务器）
                # 但在实际情况下，可能需要等待或激活新服务器
                print("Warning: No available server for job {}".format(job.job_id), file=sys.stderr)
        
        # 处理作业完成
        elif response.startswith("JCPL"):
            # 作业完成，更新状态（如果需要）
            # 这里可以记录完成信息，但当前不需要特殊处理
            pass
    
    # 发送QUIT
    sock.sendall("QUIT\n".encode())


def main():
    """主函数"""
    # 支持两种调用方式：
    # 1. python3 aras_client.py <username> [host] [port] [-n]
    # 2. python3 aras_client.py（从环境变量或默认值获取参数）
    
    use_newline = "-n" in sys.argv or "--newline" in sys.argv
    # 移除选项参数以便解析位置参数
    args = [arg for arg in sys.argv[1:] if arg not in ["-n", "--newline"]]
    
    if len(args) > 0:
        username = args[0]
        host = args[1] if len(args) > 1 else "localhost"
        port = int(args[2]) if len(args) > 2 else 50000
    else:
        # 默认值
        username = "user"
        host = "localhost"
        port = 50000
    
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((host, port))
        communicate_with_server(sock, username, use_newline)
        sock.close()
    except Exception as e:
        print("Error: {}".format(e), file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()

