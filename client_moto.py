#!/usr/bin/env python3
import socket
import argparse
import sys

class ServerInfo:
    def __init__(self, parts):
        self.type = parts[0]
        self.id = int(parts[1])
        self.state = parts[2].lower()
        self.cores = int(parts[4])
        self.mem = int(parts[5])
        self.disk = int(parts[6])
        self.waiting_jobs = int(parts[7])
        self.running_jobs = int(parts[8])
        self.load = self.waiting_jobs + self.running_jobs

    def can_host(self, c, m, d):
        return c <= self.cores and m <= self.mem and d <= self.disk

def moto_choose_server(job_req, servers):
    (_, req_core, req_mem, req_disk, est_runtime) = job_req
    best, best_score = None, float("inf")
    for s in servers:
        if not s.can_host(req_core, req_mem, req_disk):
            continue
        wait = s.load
        cost = 5000 if s.state in ["inactive", "booting"] else 0
        imbalance = s.load * 2
        score = wait + cost + imbalance
        if score < best_score:
            best_score = score
            best = (s.type, s.id)
    if not best and servers:
        s0 = servers[0]
        best = (s0.type, s0.id)
    return best

def run_client(host, port, username):
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((host, port))
        s_file = s.makefile("rwb", buffering=0)

        def send(msg: str):
            s_file.write((msg + "\n").encode())
            s_file.flush()

        def recv() -> str:
            data = s_file.readline()
            if not data:
                return ""
            return data.decode().strip()

        send("HELO")
        recv()
        send(f"AUTH {username}")
        recv()
        send("REDY")

        while True:
            msg = recv()
            if not msg:
                continue
            parts = msg.split()
            if parts[0] == "NONE":
                send("QUIT")
                recv()
                break
            elif parts[0] == "JOBN":
                job_id = int(parts[2])
                est_runtime = int(parts[3])
                req_core = int(parts[4])
                req_mem = int(parts[5])
                req_disk = int(parts[6])

                send("GETS All")
                header = recv().split()
                n = int(header[1])
                servers = []
                for _ in range(n):
                    srv = recv().split()
                    servers.append(ServerInfo(srv))
                send("OK")
                recv()

                chosen_type, chosen_id = moto_choose_server(
                    (job_id, req_core, req_mem, req_disk, est_runtime), servers
                )
                send(f"SCHD {job_id} {chosen_type} {chosen_id}")
                recv()
                send("REDY")
            elif parts[0] == "JCPL":
                send("REDY")
            else:
                send("REDY")
        s_file.close()
        s.close()
    except Exception as e:
        print(f"[ERROR] {e}", file=sys.stderr)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-p", "--port", type=int, required=True)
    parser.add_argument("-H", "--host", default="127.0.0.1")
    parser.add_argument("-u", "--user", default="student")
    args = parser.parse_args()
    print(f"[MOTO] connecting to {args.host}:{args.port} as {args.user}")
    run_client(args.host, args.port, args.user)
    print("[MOTO] finished.")
