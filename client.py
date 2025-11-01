#!/usr/bin/env python3
"""
智能作业调度客户端算法
优化平均周转时间，使用Best-Fit策略和backfilling思想
"""

import sys
import socket

# 服务器状态映射
SERVER_STATES = {
    'inactive': 0,  # 非活动
    'booting': 1,   # 启动中
    'idle': 2,       # 空闲
    'active': 3,     # 活动
    'unavailable': 4 # 不可用
}

# 连接到服务器的配置
HOST = 'localhost'
PORT = 50000


class ServerInfo:
    """服务器信息类"""
    def __init__(self, server_type, server_id, state, start_time, 
                 avail_cores, avail_mem, avail_disk, 
                 num_waiting_jobs, num_running_jobs):
        self.server_type = server_type
        self.server_id = server_id
        self.state = state
        self.start_time = int(start_time) if start_time != '-' else 0
        self.avail_cores = int(avail_cores)
        self.avail_mem = int(avail_mem)
        self.avail_disk = int(avail_disk)
        self.num_waiting_jobs = int(num_waiting_jobs)
        self.num_running_jobs = int(num_running_jobs)
        
    def is_immediately_available(self):
        """检查服务器是否立即可用（Idle或Active且有足够资源）"""
        return self.state == 'idle' or (self.state == 'active' and self.num_waiting_jobs == 0)
    
    def is_booting(self):
        """检查服务器是否正在启动"""
        return self.state == 'booting'
    
    def is_inactive(self):
        """检查服务器是否非活动"""
        return self.state == 'inactive'
    
    def get_bootup_delay(self, bootup_times):
        """获取启动延迟时间"""
        if self.is_inactive():
            return bootup_times.get(self.server_type, 60)
        elif self.is_booting():
            return max(0, self.start_time - current_time) if hasattr(self, 'current_time') else 0
        return 0


class Job:
    """作业类"""
    def __init__(self, job_id, submit_time, est_run_time, cores, memory, disk):
        self.job_id = job_id
        self.submit_time = int(submit_time)
        self.est_run_time = int(est_run_time)
        self.cores = int(cores)
        self.memory = int(memory)
        self.disk = int(disk)


def send_message(sock, message, newline=True):
    """发送消息到服务器"""
    msg = message + ('\n' if newline else '')
    sock.sendall(msg.encode())


# receive_message函数已移除，改为在主循环中直接使用socket.recv()


def parse_server_info(line):
    """解析服务器信息行"""
    parts = line.strip().split()
    if len(parts) >= 9:
        # 标准格式：server_type server_id state start_time avail_cores avail_mem avail_disk num_waiting_jobs num_running_jobs
        return ServerInfo(
            parts[0],  # server_type
            parts[1],  # server_id
            parts[2],  # state
            parts[3],  # start_time
            parts[4],  # avail_cores
            parts[5],  # avail_mem
            parts[6],  # avail_disk
            parts[7],  # num_waiting_jobs
            parts[8]   # num_running_jobs
        )
    return None


def can_fit_job(server, job):
    """检查服务器是否可以运行作业"""
    return (server.avail_cores >= job.cores and
            server.avail_mem >= job.memory and
            server.avail_disk >= job.disk)


def calculate_resource_waste(server, job):
    """计算资源浪费（越小越好）"""
    waste_cores = max(0, server.avail_cores - job.cores)
    waste_mem = max(0, server.avail_mem - job.memory)
    waste_disk = max(0, server.avail_disk - job.disk)
    
    # 归一化资源浪费（考虑总资源）
    total_cores = waste_cores + job.cores
    total_mem = waste_mem + job.memory
    total_disk = waste_disk + job.disk
    
    if total_cores == 0 or total_mem == 0 or total_disk == 0:
        return float('inf')
    
    waste_score = (waste_cores / total_cores + 
                   waste_mem / total_mem + 
                   waste_disk / total_disk) / 3.0
    
    return waste_score


def calculate_server_score(server, job, bootup_times, current_time):
    """计算服务器优先级评分（分数越高越好）"""
    if not can_fit_job(server, job):
        return -1
    
    # 基础评分
    score = 0.0
    
    # 1. 优先选择立即可用的服务器（避免启动延迟）- 权重最高
    if server.is_immediately_available():
        score += 2000.0
    elif server.is_booting():
        # 正在启动的服务器，根据剩余启动时间评分
        bootup_time = bootup_times.get(server.server_type, 60)
        if server.start_time > 0:
            estimated_delay = max(0, server.start_time - current_time)
            if estimated_delay <= bootup_time and estimated_delay >= 0:
                # 即将完成启动，给予较高评分（延迟越小，评分越高）
                score += 800.0 * (1.0 - min(1.0, estimated_delay / bootup_time))
            else:
                score += 200.0
        else:
            score += 200.0
    elif server.is_inactive():
        # 非活动服务器，启动延迟最大
        bootup_time = bootup_times.get(server.server_type, 60)
        score += 50.0  # 最低优先级
    
    # 2. 选择等待队列最短的服务器（最小化作业等待时间）
    score -= server.num_waiting_jobs * 15.0
    
    # 3. 考虑运行中作业数量（影响并行度和资源竞争）
    score -= server.num_running_jobs * 8.0
    
    # 4. Best-Fit: 最小化资源浪费（资源利用率最大化）
    resource_waste = calculate_resource_waste(server, job)
    if resource_waste < float('inf'):
        score -= resource_waste * 100.0
        
        # 5. 优先选择资源更接近作业需求的服务器（减少碎片化）
        resource_fit = 1.0 / (1.0 + resource_waste * 20.0)
        score += resource_fit * 200.0
        
        # 6. Backfilling思想：如果服务器有空闲资源可以利用，给予额外评分
        # 资源利用率高的服务器优先
        total_avail = server.avail_cores + server.avail_mem + server.avail_disk
        job_total = job.cores + job.memory + job.disk
        
        if total_avail > 0:
            utilization = job_total / total_avail
            if utilization > 0.3:  # 资源利用率合理（不低于30%）
                score += 100.0 * min(1.0, utilization)
    
    return score


def select_best_server(servers, job, bootup_times, current_time):
    """选择最适合的服务器（Best-Fit策略）"""
    valid_servers = [s for s in servers if can_fit_job(s, job)]
    
    if not valid_servers:
        return None
    
    # 计算每个服务器的评分
    scored_servers = []
    for server in valid_servers:
        score = calculate_server_score(server, job, bootup_times, current_time)
        scored_servers.append((score, server))
    
    # 按评分排序，选择最高分的
    scored_servers.sort(key=lambda x: x[0], reverse=True)
    
    return scored_servers[0][1]


def get_capable_servers(sock, job):
    """获取能够运行作业的服务器列表"""
    # 使用GETS Capable查询能够运行作业的服务器
    send_message(sock, f"GETS Capable {job.cores} {job.memory} {job.disk}")
    
    # 接收DATA响应（简化接收逻辑）
    data = sock.recv(4096).decode()
    lines = data.split('\n')
    
    # 找到DATA行
    data_line = None
    for line in lines:
        if line.strip().startswith("DATA"):
            data_line = line.strip()
            break
    
    if not data_line or not data_line.startswith("DATA"):
        # 如果没有找到DATA，尝试直接读取
        data_response = data.strip().split('\n')[0]
        if not data_response.startswith("DATA"):
            return []
        data_line = data_response
    
    # 解析DATA头: DATA <nRecs> <recLen>
    parts = data_line.split()
    n_recs = int(parts[1])
    rec_len = int(parts[2])
    
    # 发送OK确认
    send_message(sock, "OK")
    
    # 接收服务器信息（可能多行）
    servers = []
    buffer = ""
    received_count = 0
    
    while received_count < n_recs:
        # 读取更多数据
        data = sock.recv(4096).decode()
        if not data:
            break
        buffer += data
        
        # 处理buffer中的完整行
        while '\n' in buffer:
            line, buffer = buffer.split('\n', 1)
            line = line.strip()
            
            if not line or line == ".":
                # 结束标记
                break
            
            # 解析服务器信息
            server_info = parse_server_info(line)
            if server_info:
                servers.append(server_info)
                received_count += 1
            
            if received_count >= n_recs:
                break
    
    # 确保接收结束标记
    if received_count < n_recs:
        # 继续读取直到找到结束标记
        while True:
            data = sock.recv(1024).decode()
            if not data:
                break
            buffer += data
            if '.' in buffer or buffer.strip() == ".":
                break
    
    return servers


def main():
    """主函数"""
    # 检测是否需要换行符
    newline = True
    
    # 创建socket连接
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.connect((HOST, PORT))
    except Exception as e:
        print(f"连接失败: {e}", file=sys.stderr)
        sys.exit(1)
    
    try:
        # 握手协议
        # 1. 发送HELO
        send_message(sock, "HELO", newline)
        data = sock.recv(4096).decode()
        response = data.strip().split('\n')[0]
        if response != "OK":
            print(f"握手失败: 期望OK，收到{response}", file=sys.stderr)
            return
        
        # 2. 发送AUTH
        send_message(sock, "AUTH username", newline)
        data = sock.recv(4096).decode()
        response = data.strip().split('\n')[0]
        if response != "OK":
            print(f"认证失败: 期望OK，收到{response}", file=sys.stderr)
            return
        
        # 服务器启动时间（从配置中获取，这里使用默认值）
        # 实际应用中可以从系统信息文件中读取
        bootup_times = {
            'small': 60,
            'medium': 60,
            'large': 60
        }
        
        current_time = 0
        job_queue = []  # 用于backfilling的作业队列
        
        # 主调度循环
        buffer = ""
        while True:
            # 发送REDY表示准备好接收新消息
            send_message(sock, "REDY", newline)
            
            # 接收响应（可能包含多行）
            if not buffer:
                data = sock.recv(4096).decode()
                buffer = data
            else:
                data = sock.recv(4096).decode() if '\n' not in buffer else ""
                if data:
                    buffer += data
            
            # 提取第一行作为响应
            if '\n' in buffer:
                response, buffer = buffer.split('\n', 1)
                response = response.strip()
            else:
                response = buffer.strip()
                buffer = ""
            
            # 检查响应类型
            if response.startswith("JOBN"):
                # 新作业提交: JOBN <job_id> <submit_time> <est_run_time> <cores> <memory> <disk>
                parts = response.split()
                job = Job(parts[2], parts[3], parts[4], parts[5], parts[6], parts[7])
                current_time = int(parts[3])
                
                # 获取能够运行此作业的服务器
                servers = get_capable_servers(sock, job)
                
                if servers:
                    # 选择最佳服务器
                    best_server = select_best_server(servers, job, bootup_times, current_time)
                    
                    if best_server:
                        # 调度作业: SCHD <job_id> <server_type> <server_id>
                        send_message(sock, f"SCHD {job.job_id} {best_server.server_type} {best_server.server_id}", newline)
                        # 接收SCHD响应（可能是OK或其他状态）
                        data = sock.recv(4096).decode()
                        # 不处理响应，继续
                    else:
                        # 如果没有合适的服务器，选择第一个（兜底）
                        send_message(sock, f"SCHD {job.job_id} {servers[0].server_type} {servers[0].server_id}", newline)
                        data = sock.recv(4096).decode()
                        # 不处理响应，继续
                else:
                    # 如果没有可用服务器，查询所有服务器（使用Avail查询）
                    send_message(sock, f"GETS Avail {job.cores} {job.memory} {job.disk}", newline)
                    
                    # 接收DATA响应
                    data = sock.recv(4096).decode()
                    lines = data.split('\n')
                    data_line = lines[0].strip() if lines else ""
                    
                    if data_line.startswith("DATA"):
                        parts = data_line.split()
                        n_recs = int(parts[1])
                        send_message(sock, "OK")
                        
                        # 接收服务器信息
                        all_servers = []
                        recv_buffer = '\n'.join(lines[1:]) if len(lines) > 1 else ""
                        received_count = 0
                        
                        while received_count < n_recs:
                            if '\n' in recv_buffer:
                                line, recv_buffer = recv_buffer.split('\n', 1)
                                line = line.strip()
                                if line and line != ".":
                                    server_info = parse_server_info(line)
                                    if server_info and can_fit_job(server_info, job):
                                        all_servers.append(server_info)
                                        received_count += 1
                            else:
                                data = sock.recv(4096).decode()
                                if not data:
                                    break
                                recv_buffer += data
                        
                        if all_servers:
                            best_server = select_best_server(all_servers, job, bootup_times, current_time)
                            if best_server:
                                send_message(sock, f"SCHD {job.job_id} {best_server.server_type} {best_server.server_id}", newline)
                                data = sock.recv(4096).decode()
                                buffer = data
                
            elif response.startswith("JOBP"):
                # 重新提交的作业（失败后）
                parts = response.split()
                job = Job(parts[2], parts[3], parts[4], parts[5], parts[6], parts[7])
                current_time = int(parts[3])
                
                servers = get_capable_servers(sock, job)
                if servers:
                    best_server = select_best_server(servers, job, bootup_times, current_time)
                    if best_server:
                        send_message(sock, f"SCHD {job.job_id} {best_server.server_type} {best_server.server_id}", newline)
                        data = sock.recv(4096).decode()
                        # 不处理响应，继续
                
            elif response.startswith("JCPL"):
                # 作业完成: JCPL <job_id> <server_type> <server_id>
                # 更新当前时间（如果需要）
                parts = response.split()
                # 可以在这里实现backfilling：检查等待队列中的作业
                pass
                
            elif response == "NONE":
                # 没有更多作业，退出
                break
                
            elif response.startswith("RESF") or response.startswith("RESR"):
                # 资源失败/恢复，继续循环
                pass
            else:
                # 未知响应，继续处理
                pass
        
        # 发送QUIT退出
        send_message(sock, "QUIT", newline)
        
    except Exception as e:
        print(f"错误: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
    finally:
        sock.close()


if __name__ == "__main__":
    main()

