import tkinter as tk
import tkinter.filedialog as filedialog
import threading
import can
import time
import datetime
import struct
import queue
import csv
import os
import pandas as pd

# ==================== Constants ====================
SDO_REQUEST_BASE = 0x600
SDO_RESPONSE_BASE = 0x580

# ==================== SDO / NMT 辅助函数 ====================
def sdo_read_request(node_id: int, index: int, subindex: int) -> can.Message:
    can_id = SDO_REQUEST_BASE + node_id
    data = [0x40, index & 0xFF, (index >> 8) & 0xFF, subindex, 0, 0, 0, 0]
    return can.Message(arbitration_id=can_id, is_extended_id=False, data=data)

def send_sync(bus, log_queue=None):
    msg = can.Message(arbitration_id=0x80, is_extended_id=False, data=[])
    bus.send(msg)
    if log_queue:
        ts = timestamp()
        data_str = format_can_data(msg.data)
        log_queue.put(("all_msg", f"Tx {ts} ID=0x{msg.arbitration_id:03X} Data=[{data_str}]  SYNC\n"))

def send_nmt(bus, command, node_id, log_queue=None):
    msg = can.Message(arbitration_id=0x000, data=[command, node_id], is_extended_id=False)
    bus.send(msg)
    if log_queue:
        ts = timestamp()
        data_str = format_can_data(msg.data)
        log_queue.put(("all_msg", f"Tx {ts} ID=0x{msg.arbitration_id:03X} Data=[{data_str}]  NMT cmd={command} node=0x{node_id:02X}\n"))

def request_pdo(bus, pdo_cob_id, log_queue=None):
    msg = can.Message(arbitration_id=pdo_cob_id, is_extended_id=False, is_remote_frame=True, dlc=8)
    bus.send(msg)
    if log_queue:
        ts = timestamp()
        data_str = format_can_data(msg.data) if msg.data else "(RTR)"
        log_queue.put(("all_msg", f"Tx {ts} ID=0x{pdo_cob_id:03X} Data=[{data_str}]  RTR PDO\n"))

# 常用 SDO Abort Codes
SDO_ABORT_DICT = {
    0x05030000: "Toggle bit not altered",
    0x05040001: "Device state conflict",
    0x05040005: "PDO not mapped",
    0x06010000: "Unsupported access to object",
    0x06010001: "Attempt to read a write-only object",
    0x06020000: "Object does not exist in object dictionary",
    0x06040041: "Device state does not support this command",
    0x06040042: "Object is not mapped to PDO",
    0x06040043: "Parameter length mismatch",
    0x06090011: "Subindex does not exist",
    0x06090030: "Value range exceeded",
    0x06090031: "Value too high",
    0x06090032: "Value too low",
    0x08000000: "General error",
}

# ==================== Utility Functions ====================
def timestamp() -> str:
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

def format_can_data(data_bytes):
    if not data_bytes:
        return ""
    return ' '.join(f"{b:02X}" for b in data_bytes)

def parse_input_to_bytes(value_str: str, dtype: str) -> bytes:
    v = value_str.strip()
    if not v:
        raise ValueError("值不能为空")

    if dtype == "float":
        return struct.pack("<f", float(v))
    elif dtype == "hex" or dtype == "uint":
        num = int(v, 16) if v.lower().startswith("0x") else int(v)
        return struct.pack("<I", num & 0xFFFFFFFF)
    elif dtype == "int":
        num = int(v, 16) if v.lower().startswith("0x") else int(v)
        return struct.pack("<i", num)
    else:
        raise ValueError(f"不支持的类型: {dtype}")

def format_read_value(raw: int | float | bool, dtype: str, bit: int = 0) -> str:
    if dtype == "float":
        return f"{raw:.6f}"
    elif dtype == "bool":
        return str((raw >> bit) & 1)
    elif dtype == "hex":
        return f"0x{raw:08X}"
    elif dtype == "int":
        return str(struct.unpack("<i", struct.pack("<I", raw))[0])
    else:  # uint
        return str(raw)

# ==================== PDO 格式化函数 ====================
def format_pdo_message(msg: can.Message) -> str:
    if msg is None:
        return ""
    
    ts = timestamp()
    lines = [f"PDO 0x{msg.arbitration_id:X}  ({ts}):"]
    data = msg.data
    
    for i, byte_val in enumerate(data):
        bits = f"{byte_val:08b}"
        bit_groups = " ".join(bits[j:j+4] for j in range(0, 8, 4))
        lines.append(f"  Byte{i+1:2d}: 0x{byte_val:02X}   ({bit_groups})")
    
    if not data:
        lines.append("  (空报文)")
    
    return "\n".join(lines) + "\n"

class CANopenApp:
    def __init__(self, root):
        self.root = root
        self.root.title("CANopen SDO/PDO 工具")
        self.root.geometry("1000x900")
        
        self.log_queue = queue.Queue()
        self.scanning = False
        self.resp_queues = {}

        self.all_msg_buffer = []
        self.parsed_log_buffer = []
        self.pdo_buffer = []

        # ==================== 连接参数区 ====================
        conn_frame = tk.Frame(root)
        conn_frame.pack(fill="x", padx=8, pady=8)

        left_part = tk.Frame(conn_frame)
        left_part.pack(side="left")

        tk.Label(left_part, text="波特率", font=("Microsoft YaHei", 10)).pack(side="left", padx=(0, 5))
        self.bitrate_var = tk.StringVar(value="500K")
        bitrate_options = ["125K", "250K", "500K", "800K", "1000K"]
        tk.OptionMenu(left_part, self.bitrate_var, *bitrate_options).pack(side="left", padx=4)

        tk.Label(left_part, text="通道:", font=("Microsoft YaHei", 10)).pack(side="left", padx=(8, 2))
        self.channel_entry = tk.Entry(left_part, width=4)
        self.channel_entry.insert(0, "0")
        self.channel_entry.pack(side="left", padx=(4, 4))

        self.loopback_var = tk.BooleanVar(value=False)
        #tk.Checkbutton(left_part, text="回环", variable=self.loopback_var).pack(side="left", padx=(4, 0))

        self.scan_button = tk.Button(
            left_part, 
            text="扫描节点", 
            command=self.scan_nodes,
            font=("Microsoft YaHei", 10, "bold"),
            width=6,
            padx=6, pady=6
        )
        self.scan_button.pack(side="left", padx=(8, 10))

        tk.Label(left_part, text="节点:", font=("Microsoft YaHei", 10)).pack(side="left", padx=(3, 5))
        self.node_var = tk.StringVar(value="未选择")
        self.node_menu = tk.OptionMenu(left_part, self.node_var, "未选择")
        self.node_menu.config(width=5)
        self.node_menu.pack(side="left", padx=3)

        # 强制连接区域
        tk.Label(left_part, text="手动ID:", font=("Microsoft YaHei", 10,)).pack(side="left", padx=(8, 3))
        self.manual_node_entry = tk.Entry(left_part, width=5, font=("Microsoft YaHei", 10))
        self.manual_node_entry.insert(0, "2A")
        self.manual_node_entry.pack(side="left", padx=(0, 4))

        manual_connect_btn = tk.Button(
            left_part,
            text="直接连接",
            command=self.force_connect_and_open_bus,
            #bg="#d32f2f", fg="white",
            font=("Microsoft YaHei", 10),
            width=6,
            height=1,
            padx=4, pady=4
        )
        manual_connect_btn.pack(side="left", padx=(8, 0))

        self.stop_button = tk.Button(left_part, text="断开", command=self.stop, state="disabled")
        self.stop_button.pack(side="left", padx=(15, 6))

        self.status_label = tk.Label(left_part, text="未连接", font=("Microsoft YaHei", 10, "bold"), fg="red")
        self.status_label.pack(side="left", padx=(12, 0))

        author_label = tk.Label(
            conn_frame,
            text="LXF | V2.4 | Airsys",
            font=("Microsoft YaHei", 9),
            fg="#666666"
        )
        author_label.pack(side="right", padx=(0, 12))

        # ==================== SDO 区 ====================
        self.sdo_frame = tk.LabelFrame(root, text="SDO 读写", padx=8, pady=6)
        self.sdo_frame.pack(fill="x", padx=12, pady=(10, 4))

        top_ctrl = tk.Frame(self.sdo_frame)
        top_ctrl.pack(fill="x", padx=4, pady=(0,6))

        import_frame = tk.LabelFrame(top_ctrl, text="导入", padx=6, pady=4)
        import_frame.pack(side="left", padx=(0,40))
        tk.Button(import_frame, text="导入CSV/Excel", command=self.import_csv, width=14).pack()

        read_frame = tk.LabelFrame(top_ctrl, text="读取", padx=6, pady=4)
        read_frame.pack(side="left", padx=(0,8))
        tk.Button(read_frame, text="一键读取当前页", command=self.read_all_rows, width=16).pack(side="left", padx=(0,6))
        self.sdo_loop_var = tk.BooleanVar(value=False)
        tk.Checkbutton(read_frame, text="循环读取", variable=self.sdo_loop_var, command=self.toggle_sdo_loop).pack(side="left")
        tk.Label(read_frame, text="间隔(s):", font=("Microsoft YaHei", 9)).pack(side="left", padx=(8,2))
        self.sdo_loop_interval = tk.Entry(read_frame, width=6)
        self.sdo_loop_interval.insert(0, "1")
        self.sdo_loop_interval.pack(side="left")

        page_frame = tk.LabelFrame(top_ctrl, text="页码", padx=6, pady=4)
        page_frame.pack(side="left", padx=(40, 0))
        tk.Button(page_frame, text="上一页", command=self.prev_page, width=8).pack(side="left", padx=4)
        self.page_label = tk.Label(page_frame, text="第 1 / 1 页 (共 0 行)", font=("Microsoft YaHei", 9))
        self.page_label.pack(side="left", padx=4)
        tk.Button(page_frame, text="下一页", command=self.next_page, width=8).pack(side="left", padx=4)
        self.jump_entry = tk.Entry(page_frame, width=6)
        self.jump_entry.pack(side="left", padx=(0,4))
        tk.Button(page_frame, text="跳转", command=self.jump_to_page, width=4).pack(side="left")

        right_frame = tk.Frame(self.sdo_frame)
        right_frame.pack(fill="x", expand=True)

        self.sdo_rows = []
        self.all_sdo_data = []
        self.current_page = 1
        self.rows_per_page = 5
        self.total_pages = 1

        dtype_options = ["hex", "int", "uint", "float", "bool"]

        for i in range(self.rows_per_page):
            row = tk.Frame(right_frame)
            row.pack(fill="x", pady=2)

            row_label = tk.Label(row, text=f"行 {i+1}", width=6, anchor="w", font=("Microsoft YaHei",9))
            row_label.pack(side="left")

            tk.Label(row, text="Index:").pack(side="left", padx=(8, 2))
            idx_entry = tk.Entry(row, width=10)
            idx_entry.insert(0, "2002")
            idx_entry.pack(side="left")

            tk.Label(row, text="Sub:").pack(side="left", padx=(8, 2))
            sub_entry = tk.Entry(row, width=5)
            sub_entry.insert(0, "0")
            sub_entry.pack(side="left")

            tk.Label(row, text="Type:").pack(side="left", padx=(8, 2))
            dtype_var = tk.StringVar(value="uint")
            menu = tk.OptionMenu(row, dtype_var, *dtype_options)
            menu.config(width=8)
            menu.pack(side="left")

            tk.Label(row, text="Bit:").pack(side="left", padx=(8, 2))
            bit_entry = tk.Entry(row, width=4)
            bit_entry.insert(0, "0")
            bit_entry.pack(side="left")

            read_btn = tk.Button(row, text="读", width=6, command=lambda x=i: self.read_sdo_gui(x))
            read_btn.pack(side="left", padx=(8, 4))

            read_result = tk.Label(row, text="——", width=10, anchor="w", fg="blue")
            read_result.pack(side="left", padx=(4, 8))

            write_btn = tk.Button(row, text="写", width=6, command=lambda x=i: self.write_sdo_gui(x))
            write_btn.pack(side="left", padx=(8, 4))

            write_entry = tk.Entry(row, width=12)
            write_entry.pack(side="left", padx=(4, 8))

            comment_label = tk.Label(row, text="——", anchor="w", fg="#555", font=("Microsoft YaHei", 9))
            comment_label.pack(side="left", padx=(20, 12))

            self.sdo_rows.append({
                'row_label': row_label,
                'index': idx_entry, 'sub': sub_entry, 'dtype': dtype_var,
                'bit': bit_entry, 'write': write_entry,
                'read_label': read_result,
                'comment_label': comment_label,
            })

        # ==================== 报文显示区 ====================
        msg_frame = tk.Frame(root)
        msg_frame.pack(fill="both", expand=True, padx=12, pady=8)

        pdo_frame = tk.LabelFrame(msg_frame, text="PDO 报文（字节 & 位）", padx=6, pady=6)
        pdo_frame.pack(side="left", fill="y", expand=False, padx=(0, 6), ipadx=0)
        pdo_frame.pack_propagate(False)
        pdo_frame.config(width=320)

        pdo_ctrl = tk.Frame(pdo_frame)
        pdo_ctrl.pack(fill="x", pady=(0,6))

        sync_group = tk.Frame(pdo_ctrl)
        sync_group.pack(side="left", padx=(0, 12))
        tk.Button(sync_group, text="发送 SYNC", command=self.send_sync_gui, width=10).pack(side="left")

        pdo_group = tk.Frame(pdo_ctrl)
        pdo_group.pack(side="left", padx=(12, 0))
        tk.Label(pdo_group, text="COB-ID:", font=("Microsoft YaHei", 10)).pack(side="left", padx=(0, 5))
        self.pdo_cob_id_entry = tk.Entry(pdo_group, width=6)
        self.pdo_cob_id_entry.insert(0, "180")
        self.pdo_cob_id_entry.pack(side="left", padx=(0, 6))
        tk.Button(pdo_group, text="请求 PDO", command=self.request_pdo_gui, width=8).pack(side="left")

        self.pdo_text = tk.Text(pdo_frame, height=12, font=("Consolas", 10), bg="#fdfdf5")
        self.pdo_text.pack(fill="both", expand=True)

        all_frame = tk.LabelFrame(msg_frame, text="所有报文", padx=6, pady=6)
        all_frame.pack(side="right", fill="both", expand=True, padx=(6, 0))

        send_ctrl = tk.Frame(all_frame)
        send_ctrl.pack(fill="x", pady=(0, 6))

        line1 = tk.Frame(send_ctrl)
        line1.pack(fill="x", pady=(0, 4))

        tk.Label(line1, text="ID(0x):", font=("Microsoft YaHei", 10)).pack(side="left", padx=(0, 4))
        self.send_id_entry = tk.Entry(line1, width=12)
        self.send_id_entry.insert(0, "601")
        self.send_id_entry.pack(side="left", padx=(0, 12))

        tk.Label(line1, text="DLC:", font=("Microsoft YaHei", 10)).pack(side="left", padx=(0, 4))
        self.dlc_entry = tk.Entry(line1, width=4)
        self.dlc_entry.insert(0, "")
        self.dlc_entry.pack(side="left", padx=(0, 12))

        tk.Label(line1, text="Data:", font=("Microsoft YaHei", 10)).pack(side="left", padx=(0, 4))
        self.send_data_entry = tk.Entry(line1, width=40)
        self.send_data_entry.insert(0, "")
        self.send_data_entry.pack(side="left", fill="x", expand=False, padx=(0, 8))

        line2 = tk.Frame(send_ctrl)
        line2.pack(fill="x")

        self.rtr_var = tk.BooleanVar(value=False)
        tk.Checkbutton(line2, text="RTR (远程帧)", variable=self.rtr_var).pack(side="left", padx=(0, 12))

        self.ext_id_var = tk.BooleanVar(value=False)
        tk.Checkbutton(line2, text="扩展ID", variable=self.ext_id_var).pack(side="left", padx=(0, 12))

        tk.Label(line2, text="预设:", font=("Microsoft YaHei", 10)).pack(side="left", padx=(0, 4))
        self.preset_var = tk.StringVar(value="自定义")
        preset_options = [
            "自定义",
            "SYNC",
            "NMT Operational 广播",
            "NMT Operational 节点",
            "NMT Reset 节点",
            "RTR PDO 示例"
        ]
        preset_menu = tk.OptionMenu(line2, self.preset_var, *preset_options, command=self.on_preset_select)
        preset_menu.config(width=18)
        preset_menu.pack(side="left", padx=(0, 12))

        tk.Button(line2, text="发送", width=8, command=self.send_custom_can).pack(side="left", padx=(8, 0))

        self.all_msg_text = tk.Text(all_frame, height=12, font=("Consolas", 10))
        self.all_msg_text.pack(fill="both", expand=True)

        # ==================== 日志区 ====================
        log_header = tk.Frame(root)
        log_header.pack(fill="x", padx=12, pady=(8,0))
        tk.Label(log_header, text="程序日志：", font=("Microsoft YaHei", 10)).pack(side="left")

        self.follow_var = tk.BooleanVar(value=True)
        follow_check = tk.Checkbutton(log_header, text="跟随最新日志", variable=self.follow_var)
        follow_check.pack(side="left", padx=(20, 0))

        tk.Button(log_header, text="清空日志", command=lambda: self.log_text.delete("1.0", "end")).pack(side="left", padx=(20, 0))

        tk.Button(log_header, text="保存程序日志", command=self.save_log).pack(side="right")

        log_frame = tk.Frame(root)
        log_frame.pack(fill="both", expand=True, padx=12, pady=(0, 12))

        log_scrollbar = tk.Scrollbar(log_frame)
        log_scrollbar.pack(side="right", fill="y")

        self.log_text = tk.Text(log_frame, height=10, font=("Consolas", 10), yscrollcommand=log_scrollbar.set)
        self.log_text.pack(side="left", fill="both", expand=True)

        log_scrollbar.config(command=self.log_text.yview)

        self.bus = None
        self.running = False
        self.default_node = None
        self.known_nodes = set()

        self.root.after(100, self.process_queue)

        self.node_var.trace_add("write", lambda *args: self._auto_connect_selected_node())

    def on_preset_select(self, *args):
        preset = self.preset_var.get()
        if preset == "自定义":
            return
        elif preset == "SYNC":
            self.send_id_entry.delete(0, tk.END)
            self.send_id_entry.insert(0, "80")
            self.send_data_entry.delete(0, tk.END)
            self.rtr_var.set(False)
        elif preset == "NMT Operational 广播":
            self.send_id_entry.delete(0, tk.END)
            self.send_id_entry.insert(0, "0")
            self.send_data_entry.delete(0, tk.END)
            self.send_data_entry.insert(0, "01 00")
            self.rtr_var.set(False)
        elif preset == "NMT Operational 节点":
            self.send_id_entry.delete(0, tk.END)
            self.send_id_entry.insert(0, "0")
            self.send_data_entry.delete(0, tk.END)
            self.send_data_entry.insert(0, "01 2A")
            self.rtr_var.set(False)
        elif preset == "NMT Reset 节点":
            self.send_id_entry.delete(0, tk.END)
            self.send_id_entry.insert(0, "0")
            self.send_data_entry.delete(0, tk.END)
            self.send_data_entry.insert(0, "81 2A")
            self.rtr_var.set(False)
        elif preset == "RTR PDO 示例":
            self.send_id_entry.delete(0, tk.END)
            self.send_id_entry.insert(0, "1AA")
            self.send_data_entry.delete(0, tk.END)
            self.rtr_var.set(True)

    def process_queue(self):
        try:
            while not self.log_queue.empty():
                msg_type, text = self.log_queue.get_nowait()
                if msg_type == "pdo":
                    self.pdo_buffer.append(text)
                elif msg_type == "all_msg":
                    self.all_msg_buffer.append(text)
                elif msg_type == "parsed_log":
                    if not self.scanning:
                        self.parsed_log_buffer.append(text + "\n")
                elif msg_type == "log":
                    self.parsed_log_buffer.append(text)
        except queue.Empty:
            pass

        if self.pdo_buffer:
            self.pdo_text.insert("end", "".join(self.pdo_buffer))
            self.pdo_text.see("end")
            self.pdo_buffer.clear()

        if self.all_msg_buffer:
            self.all_msg_text.insert("end", "".join(self.all_msg_buffer))
            self.all_msg_text.see("end")
            self.all_msg_buffer.clear()

        if self.parsed_log_buffer:
            self.log_text.insert("end", "".join(self.parsed_log_buffer))
            if self.follow_var.get():
                self.log_text.see("end")
            self.parsed_log_buffer.clear()

        self.root.after(50, self.process_queue)

    def log(self, text):
        ts = timestamp()
        self.log_queue.put(("log", f"{ts} {text}\n"))

    def update_connection_status(self):
        if self.default_node is not None:
            self.status_label.config(text=f"已连接到 0x{self.default_node:02X} 节点", fg="green")
        else:
            self.status_label.config(text="未连接", fg="red")

    def force_connect_and_open_bus(self):
        node_str = self.manual_node_entry.get().strip()
        if not node_str:
            self.log("请输入节点ID（如 2A 或 0x2A）")
            return

        try:
            node_id = int(node_str, 16) if node_str.lower().startswith("0x") else int(node_str, 16)
            if not (1 <= node_id <= 0x7F):
                self.log(f"节点ID 无效（范围 1~127）：{node_str}")
                return
        except ValueError:
            self.log(f"节点ID 格式错误：{node_str}")
            return

        if self.bus is None or not self.running:
            self._open_can_bus()

        if self.bus is None:
            return

        self.default_node = node_id
        self.node_var.set(f"0x{node_id:02X}")
        send_nmt(self.bus, 0x01, node_id, self.log_queue)
        time.sleep(0.15)
        self.log(f"已直接连接到节点 0x{node_id:02X}")
        self.update_connection_status()

    def scan_nodes(self):
        if self.bus is None or not self.running:
            self._open_can_bus()
            if self.bus is None:
                return

        self.scan_button.config(state="disabled")
        self.log("开始扫描节点...")
        self.status_label.config(text="正在扫描...", fg="orange")
        threading.Thread(target=self._perform_scan_in_thread, daemon=True).start()

    def _open_can_bus(self):
        try:
            bitrate_str = self.bitrate_var.get()
            bitrate = int(bitrate_str[:-1]) * 1000
            loopback_enabled = self.loopback_var.get()

            ch_str = self.channel_entry.get().strip() or "0"
            channel_val = int(ch_str) if ch_str.isdigit() else ch_str

            self.bus = can.Bus(
                interface='gs_usb',
                channel=channel_val,
                bitrate=bitrate,
                loopback=loopback_enabled
            )

            self.log(f"CAN 总线已打开，波特率: {bitrate_str}，通道: {channel_val}，回环: {'开启' if loopback_enabled else '关闭'}")

            self.running = True
            self.receiver_thread = threading.Thread(target=self._receiver_loop, daemon=True)
            self.receiver_thread.start()

            self.stop_button.config(state="normal")

        except Exception as e:
            self.log(f"打开 CAN 总线失败: {str(e)}")
            self.status_label.config(text=f"总线打开失败: {str(e)}", fg="red")

    def _perform_scan_in_thread(self):
        try:
            self.scanning = True
            found_nodes = {}

            for node_id in range(1, 128):
                req = sdo_read_request(node_id, 0x1000, 0x00)
                resp_id = SDO_RESPONSE_BASE + node_id
                q = queue.Queue()
                self.resp_queues[resp_id] = q
                
                ts = timestamp()
                data_str = format_can_data(req.data)
                self.log_queue.put(("all_msg", f"Tx {ts} ID=0x{req.arbitration_id:03X} Data=[{data_str}]  (扫描节点 {node_id})\n"))
                
                try:
                    self.bus.send(req)
                    try:
                        resp = q.get(timeout=0.03)  # 改为 30ms，更容易扫到
                        if resp and (resp.data[0] & 0xE0 == 0x40):
                            resp_str = format_can_data(resp.data)
                            self.log_queue.put(("all_msg", f"Rx {ts} ID=0x{resp.arbitration_id:03X} Data=[{resp_str}]\n"))
                            value = int.from_bytes(resp.data[4:8], 'little')
                            found_nodes[node_id] = value
                    except queue.Empty:
                        pass
                finally:
                    self.resp_queues.pop(resp_id, None)

            self.scanning = False

            if found_nodes:
                self.known_nodes = set(found_nodes.keys())
                node_options = [f"0x{nid:02X}" for nid in sorted(found_nodes.keys())]
                self.root.after(0, lambda: self._update_node_menu(node_options))
                self.log(f"扫描完成，发现 {len(found_nodes)} 个节点: {', '.join(node_options)}")

                operational_count = 0
                for nid in sorted(found_nodes.keys()):
                    try:
                        send_nmt(self.bus, 0x01, nid, self.log_queue)
                        operational_count += 1
                        time.sleep(0.02)
                    except Exception as e:
                        self.log(f"节点 0x{nid:02X} Operational 失败: {str(e)}")

                if operational_count > 0:
                    self.log(f"已自动 Operational {operational_count} 个节点")
            else:
                self.log("未发现任何节点")

        except Exception as e:
            self.log(f"扫描失败: {str(e)}")

        finally:
            self.root.after(0, lambda: self.scan_button.config(state="normal"))
            self.root.after(0, lambda: self.update_connection_status())

    def _update_node_menu(self, node_options):
        self.node_menu['menu'].delete(0, 'end')
        for option in node_options:
            self.node_menu['menu'].add_command(label=option, command=tk._setit(self.node_var, option))
        
        if node_options:
            self.node_var.set(node_options[0])
        else:
            self.node_var.set("未选择")

    def _auto_connect_selected_node(self):
        selected = self.node_var.get()
        if selected in ("未选择", "未扫描", ""):
            self.default_node = None
            self.update_connection_status()
            return

        try:
            node_id = int(selected, 16)
            if self.default_node == node_id:
                return
            self.default_node = node_id
            
            send_nmt(self.bus, 0x01, node_id, self.log_queue)
            
            self.log(f"已连接节点 0x{node_id:02X}")
            self.update_connection_status()
            
        except Exception as e:
            self.log(f"连接节点失败 ({selected}): {str(e)}")
            self.default_node = None
            self.update_connection_status()

    def _receiver_loop(self):
        while self.running:
            try:
                msg = self.bus.recv(timeout=0.1)
            except Exception:
                break
            if not msg:
                continue
            ts = timestamp()
            q = self.resp_queues.get(msg.arbitration_id)
            if q:
                q.put(msg)
                continue
            data_str = format_can_data(msg.data)
            self.log_queue.put(("all_msg", f"Rx {ts} ID=0x{msg.arbitration_id:03X} Data=[{data_str}]\n"))
            
            if 0x180 <= msg.arbitration_id <= 0x57F:
                formatted = format_pdo_message(msg)
                self.log_queue.put(("pdo", formatted))
                hex_vals = format_can_data(msg.data)
                parsed = f"{ts} PDO 0x{msg.arbitration_id:X} DATA {hex_vals}"
                self.log_queue.put(("parsed_log", parsed))

    def _send_and_wait_resp(self, arb_id, msg, timeout=0.05):
        q = queue.Queue()
        self.resp_queues[arb_id] = q
        try:
            self.bus.send(msg)
            ts = timestamp()
            data_str = format_can_data(msg.data)
            self.log_queue.put(("all_msg", f"Tx {ts} ID=0x{msg.arbitration_id:03X} Data=[{data_str}]\n"))
            
            resp = q.get(timeout=timeout)
            ts = timestamp()
            resp_str = format_can_data(resp.data)
            self.log_queue.put(("all_msg", f"Rx {ts} ID=0x{resp.arbitration_id:03X} Data=[{resp_str}]\n"))
            return resp
        except queue.Empty:
            raise TimeoutError("响应超时")
        finally:
            self.resp_queues.pop(arb_id, None)

    def read_sdo_block(self, node_id: int, index: int, subindex: int = 0x00, dtype: str = "uint32", timeout=0.05):
        req = sdo_read_request(node_id, index, subindex)
        resp_id = SDO_RESPONSE_BASE + node_id
        
        try:
            resp = self._send_and_wait_resp(resp_id, req, timeout=timeout)
            
            if resp.data and resp.data[0] == 0x80:
                abort_code = int.from_bytes(resp.data[4:8], "little")
                meaning = SDO_ABORT_DICT.get(abort_code, "未知中止码")
                raise Exception(f"SDO Abort: 0x{abort_code:08X} ({meaning})")
            
            cmd = resp.data[0] & 0xE0 if resp.data else 0
            if cmd not in (0x40, 0x42, 0x43, 0x47, 0x4B, 0x4F, 0x60):
                raise Exception(f"非预期的 SDO 响应命令: 0x{resp.data[0]:02X}")
            
            raw_bytes = bytes(resp.data[4:8])
            
            if dtype == "float":
                return struct.unpack("<f", raw_bytes)[0]
            elif dtype == "bool":
                return raw_bytes[0]
            elif dtype in ("hex", "uint"):
                return int.from_bytes(raw_bytes, "little")
            elif dtype == "int":
                return struct.unpack("<i", raw_bytes)[0]
            else:
                raise ValueError(f"不支持的类型: {dtype}")
                
        except queue.Empty:
            raise TimeoutError("读取超时")
        except Exception as e:
            self.log(f"SDO 读取失败 (节点 0x{node_id:02X}, 0x{index:04X}:{subindex}): {str(e)}")
            raise

    def write_sdo_block(self, node_id: int, index: int, subindex: int, value_bytes: bytes, timeout=0.05):
        length = len(value_bytes)
        if not 1 <= length <= 4:
            raise ValueError("value_bytes 长度需1-4字节")
        cmd = {1: 0x2F, 2: 0x2B, 3: 0x27, 4: 0x23}[length]
        data = [cmd, index & 0xFF, (index >> 8) & 0xFF, subindex] + list(value_bytes) + [0] * (4 - length)
        msg = can.Message(arbitration_id=SDO_REQUEST_BASE + node_id, is_extended_id=False, data=data)
        resp_id = SDO_RESPONSE_BASE + node_id
        try:
            resp = self._send_and_wait_resp(resp_id, msg, timeout=timeout)
            if resp.data[0] == 0x60:
                return True
            else:
                abort_code = int.from_bytes(bytes(resp.data[4:8]), "little")
                meaning = SDO_ABORT_DICT.get(abort_code, "未知错误")
                raise Exception(f"SDO 写入被拒绝: 0x{abort_code:08X} ({meaning})")
        except queue.Empty:
            raise TimeoutError("写入超时")

    def save_log(self):
        try:
            content = self.log_text.get("1.0", "end").strip()
            if not content:
                self.log("日志为空，未保存")
                return
            default_name = datetime.datetime.now().strftime("CANlog_%Y%m%d_%H%M%S.txt")
            path = filedialog.asksaveasfilename(defaultextension='.txt', initialfile=default_name, filetypes=[('Text files','*.txt'), ('All files','*.*')])
            if not path:
                return
            with open(path, 'w', encoding='utf-8') as f:
                f.write(content)
            self.log(f"已保存日志到 {path}")
        except Exception as e:
            self.log(f"保存日志失败: {str(e)}")

    def import_csv(self):
        file_path = filedialog.askopenfilename(filetypes=[("CSV/Excel files", "*.csv;*.xlsx;*.xls")])
        if not file_path:
            return

        try:
            if file_path.lower().endswith(('.xlsx', '.xls')):
                df = pd.read_excel(file_path, header=None)
                rows = df.values.tolist()
            else:
                with open(file_path, newline='', encoding='utf-8') as csvfile:
                    reader = csv.reader(csvfile)
                    rows = list(reader)

            if len(rows) == 0:
                self.log("文件为空")
                return

            start_row = 1 if len(rows) > 0 and str(rows[0][0]).strip().lower() in ["index", "索引"] else 0

            self.all_sdo_data = []
            for row in rows[start_row:]:
                if len(row) < 3:
                    continue
                idx_hex = str(row[0]).strip()
                sub = str(row[1]).strip()
                dtype_raw = str(row[2]).strip().lower()
                bit = str(row[3]).strip() if len(row) > 3 else "0"
                comment = str(row[4]).strip() if len(row) > 4 else "——"

                dtype_map = {"hex": "hex", "int": "int", "uint": "uint", "float": "float", "bool": "bool"}
                dtype = dtype_map.get(dtype_raw, "uint")

                self.all_sdo_data.append({
                    'index': idx_hex,
                    'sub': sub,
                    'dtype': dtype,
                    'bit': bit,
                    'comment': comment
                })

            self.current_page = 1
            self.update_page_display(force_refresh=True)
            total = len(self.all_sdo_data)
            self.log(f"成功导入 {total} 行配置，已刷新显示第 1 页")

        except Exception as e:
            self.log(f"导入文件失败: {str(e)}")

    def update_page_display(self, force_refresh=False):
        total_rows = len(self.all_sdo_data)
        self.total_pages = max(1, (total_rows + self.rows_per_page - 1) // self.rows_per_page)

        start_idx = (self.current_page - 1) * self.rows_per_page
        page_data = self.all_sdo_data[start_idx:start_idx + self.rows_per_page]

        for entry in self.sdo_rows:
            entry['row_label'].config(text="行 ——")
            entry['index'].delete(0, tk.END)
            entry['index'].insert(0, "2002")
            entry['sub'].delete(0, tk.END)
            entry['sub'].insert(0, "0")
            entry['dtype'].set("uint")
            entry['bit'].delete(0, tk.END)
            entry['bit'].insert(0, "0")
            entry['write'].delete(0, tk.END)
            entry['read_label'].config(text="——", fg="blue")
            entry['comment_label'].config(text="——")

        for i, data in enumerate(page_data):
            if i >= len(self.sdo_rows):
                break
            entry = self.sdo_rows[i]
            entry_num = start_idx + i + 1
            entry['row_label'].config(text=f"行 {entry_num}")
            entry['index'].delete(0, tk.END)
            entry['index'].insert(0, data['index'])
            entry['sub'].delete(0, tk.END)
            entry['sub'].insert(0, data['sub'])
            entry['dtype'].set(data['dtype'])
            entry['bit'].delete(0, tk.END)
            entry['bit'].insert(0, data['bit'])
            entry['comment_label'].config(text=data['comment'])

        self.page_label.config(text=f"第 {self.current_page} / {self.total_pages} 页 (共 {total_rows} 行)")

    def prev_page(self):
        if self.current_page > 1:
            self.current_page -= 1
            self.update_page_display()

    def next_page(self):
        if self.current_page < self.total_pages:
            self.current_page += 1
            self.update_page_display()

    def jump_to_page(self):
        try:
            page = int(self.jump_entry.get())
            if 1 <= page <= self.total_pages:
                self.current_page = page
                self.update_page_display()
            else:
                self.log(f"跳转页码无效：{page} (范围 1-{self.total_pages})")
        except:
            self.log("跳转页码必须是数字")

    def toggle_sdo_loop(self):
        if self.sdo_loop_var.get():
            try:
                interval = float(self.sdo_loop_interval.get())
                if interval < 0.1:
                    interval = 0.1
                    self.sdo_loop_interval.delete(0, tk.END)
                    self.sdo_loop_interval.insert(0, "0.1")
            except:
                interval = 1
                self.sdo_loop_interval.delete(0, tk.END)
                self.sdo_loop_interval.insert(0, "1")

            self.sdo_loop_running = True
            self.sdo_loop_thread = threading.Thread(target=self.sdo_loop_task, args=(interval,), daemon=True)
            self.sdo_loop_thread.start()
            self.log(f"启动 SDO 循环读取，间隔 {interval} 秒")
        else:
            self.sdo_loop_running = False
            self.log("停止 SDO 循环读取")

    def sdo_loop_task(self, interval):
        while self.sdo_loop_running:
            self.read_all_rows()
            time.sleep(interval)

    def read_sdo_gui(self, row_idx, update_log=True):
        if not self.bus or not self.default_node:
            if update_log:
                self.log("× 未连接或无节点")
            return None

        row = self.sdo_rows[row_idx]
        try:
            node = self.default_node
            idx = int(row['index'].get(), 16)
            sub = int(row['sub'].get())
            dtype = row['dtype'].get().lower()
            bit = int(row['bit'].get()) if dtype == "bool" else 0

            raw = self.read_sdo_block(node, idx, sub, dtype)
            show = format_read_value(raw, dtype, bit)

            row['read_label'].config(text=show, fg="blue")
            if update_log:
                ts = timestamp()
                bit_info = f"bit {bit}" if dtype == 'bool' else ''
                self.log_queue.put(("parsed_log", f"{ts} Index 0x{idx:04X} sub {sub} {bit_info} {dtype} {show}"))
            return show

        except Exception as e:
            row['read_label'].config(text="错误", fg="red")
            if update_log:
                self.log(f"读失败 行{row_idx+1} (备注: {row['comment_label'].cget('text')}): {str(e)}")
            return None

    def read_all_rows(self):
        self.status_label.config(text="正在读取当前页...")
        self.root.update_idletasks()
        for i in range(self.rows_per_page):
            self.read_sdo_gui(i, update_log=True)
        self.status_label.config(text=f"已连接到 0x{self.default_node:02X} 节点" if self.default_node else "未连接", fg="green" if self.default_node else "red")
        self.log(f"读取当前页 (第 {self.current_page} 页) 完成")

    def write_sdo_gui(self, row_idx):
        if not self.bus or not self.default_node:
            self.log("× 未连接或无节点")
            return

        row = self.sdo_rows[row_idx]
        try:
            node = self.default_node
            idx = int(row['index'].get(), 16)
            sub = int(row['sub'].get())
            dtype = row['dtype'].get().lower()
            inp = row['write'].get().strip()

            if dtype == "bool":
                bit = int(row['bit'].get())
                current = self.read_sdo_block(node, idx, sub, "bool")
                new_v = current | (1 << bit) if inp.lower() in ("1", "true", "on", "yes") else current & ~(1 << bit)
                bytes_val = bytes([new_v & 0xFF])
            else:
                bytes_val = parse_input_to_bytes(inp, dtype)

            success = self.write_sdo_block(node, idx, sub, bytes_val)
            ts = timestamp()
            if success:
                self.log_queue.put(("parsed_log", f"{ts} Write 0x{idx:04X} sub {sub} {dtype} {inp} success"))
            else:
                self.log_queue.put(("parsed_log", f"{ts} Write 0x{idx:04X} sub {sub} {dtype} {inp} FAILED"))

        except Exception as e:
            self.log(f"写异常 行{row_idx+1} (备注: {row['comment_label'].cget('text')}): {str(e)}")

    def send_sync_gui(self):
        if not self.bus:
            self.log("× 未连接CAN总线")
            return

        try:
            send_sync(self.bus, self.log_queue)
            self.log("已发送SYNC报文，触发同步PDO传输")
        except Exception as e:
            self.log(f"发送SYNC失败: {str(e)}")

    def request_pdo_gui(self):
        if not self.bus:
            self.log("× 未连接CAN总线")
            return

        try:
            cob_id_str = self.pdo_cob_id_entry.get().strip()
            if not cob_id_str:
                self.log("请输入PDO COB-ID")
                return

            cob_id = int(cob_id_str, 16)
            request_pdo(self.bus, cob_id, self.log_queue)
            self.log(f"已发送RTR请求PDO 0x{cob_id:03X}")
        except ValueError:
            self.log("PDO COB-ID格式错误，请输入十六进制数")
        except Exception as e:
            self.log(f"请求PDO失败: {str(e)}")

    def send_custom_can(self):
        if not self.bus:
            self.log("× CAN总线未连接，无法发送")
            return

        try:
            id_str = self.send_id_entry.get().strip()
            can_id = int(id_str, 16) if id_str.lower().startswith("0x") else int(id_str, 16)

            if not (0 <= can_id <= 0x1FFFFFFF):
                self.log("CAN ID 超出范围")
                return

            data_str = self.send_data_entry.get().strip()
            data = []
            if data_str:
                parts = data_str.split()
                if len(parts) > 8:
                    self.log("数据超过8字节，已截断")
                    parts = parts[:8]
                for part in parts:
                    b = int(part, 16)
                    if not 0 <= b <= 0xFF:
                        raise ValueError(f"无效字节: {part}")
                    data.append(b)

            is_rtr = self.rtr_var.get()
            if is_rtr:
                if not data:
                    data = [0] * 8
                else:
                    self.log("RTR 模式下 Data 将被忽略，已强制填充 8 个 00")
                    data = [0] * 8

            dlc_str = self.dlc_entry.get().strip()
            custom_dlc = None
            if dlc_str:
                try:
                    custom_dlc = int(dlc_str)
                    if not 0 <= custom_dlc <= 8:
                        raise ValueError
                except:
                    self.log("DLC 必须是 0~8 的整数，已使用数据长度")
                    custom_dlc = None

            final_dlc = custom_dlc if custom_dlc is not None else len(data)

            msg = can.Message(
                arbitration_id=can_id,
                is_extended_id=self.ext_id_var.get(),
                data=data,
                is_remote_frame=is_rtr,
                dlc=final_dlc
            )

            self.bus.send(msg)

            ts = timestamp()
            data_show = format_can_data(data) if data else "(空)"
            if is_rtr:
                data_show = "(RTR)"
            log_line = f"Tx {ts} ID=0x{can_id:X} Data=[{data_show}] DLC={final_dlc}  (手动发送)\n"
            self.log_queue.put(("all_msg", log_line))
            self.log(f"已手动发送 → 0x{can_id:X} [{data_show}] DLC={final_dlc}")

            if 0x600 <= can_id <= 0x67F and not is_rtr:
                node_id = can_id - 0x600
                resp_id = 0x580 + node_id
                q = queue.Queue()
                self.resp_queues[resp_id] = q
                try:
                    resp = q.get(timeout=0.3)
                    ts_resp = timestamp()
                    resp_show = format_can_data(resp.data)
                    self.log_queue.put(("all_msg", f"Rx {ts_resp} ID=0x{resp.arbitration_id:X} Data=[{resp_show}] (手动SDO响应)\n"))
                    self.log(f"手动SDO请求收到响应: 0x{resp.arbitration_id:X} [{resp_show}]")
                except queue.Empty:
                    self.log("手动SDO请求无响应（超时）")
                finally:
                    self.resp_queues.pop(resp_id, None)

        except ValueError as ve:
            self.log(f"输入格式错误：{str(ve)}")
        except Exception as e:
            self.log(f"发送自定义报文失败：{str(e)}")

    def stop(self):
        self.running = False
        self.sdo_loop_running = False
        self.scan_button.config(state="normal")
        self.stop_button.config(state="disabled")
        self.default_node = None
        self.node_var.set("未选择")
        self.update_connection_status()
        if self.bus:
            try:
                self.bus.shutdown()
            except:
                pass
            self.bus = None
        self.log("已断开连接")

if __name__ == "__main__":
    root = tk.Tk()
    app = CANopenApp(root)
    def on_closing():
        app.stop()
        root.destroy()
    
    root.protocol("WM_DELETE_WINDOW", on_closing)
    root.mainloop()