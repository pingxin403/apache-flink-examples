#!/usr/bin/env python3
"""
Socket 服务器 - 用于 Flink WordCount 示例

功能：
- 监听本地 9999 端口
- 接收用户输入的文本
- 将文本发送给连接的客户端（Flink 作业）

使用方法：
1. 运行此脚本：python socket_server.py
2. 运行 Flink WordCount 作业
3. 在此脚本的终端输入文本，观察 Flink 的实时统计结果

作者：韩云朋
版本：1.0
日期：2024-01-15
"""

import socket
import sys

def main():
    # 创建 TCP Socket
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    
    # 设置 SO_REUSEADDR 选项，避免 "Address already in use" 错误
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    
    # 绑定地址和端口
    host = 'localhost'
    port = 9999
    
    try:
        server.bind((host, port))
        print(f"✅ Socket server started on {host}:{port}")
        print("📡 Waiting for Flink client to connect...")
        
        # 开始监听（最多 1 个连接）
        server.listen(1)
        
        # 接受连接
        conn, addr = server.accept()
        print(f"✅ Connected by {addr}")
        print("\n" + "="*50)
        print("💡 Now you can type text and press Enter")
        print("💡 The text will be sent to Flink for real-time word counting")
        print("💡 Press Ctrl+C to stop")
        print("="*50 + "\n")
        
        # 持续接收用户输入并发送
        while True:
            try:
                # 读取用户输入
                data = input("Enter text: ")
                
                # 如果输入为空，跳过
                if not data.strip():
                    continue
                
                # 发送数据（添加换行符）
                conn.sendall((data + "\n").encode('utf-8'))
                print(f"✅ Sent: {data}")
                
            except EOFError:
                # 用户按下 Ctrl+D
                print("\n👋 EOF detected, closing connection...")
                break
                
    except KeyboardInterrupt:
        print("\n\n👋 Keyboard interrupt detected, shutting down...")
        
    except OSError as e:
        if e.errno == 48:  # Address already in use
            print(f"❌ Error: Port {port} is already in use")
            print(f"💡 Try: lsof -ti:{port} | xargs kill -9")
        else:
            print(f"❌ Error: {e}")
        sys.exit(1)
        
    finally:
        # 关闭连接
        try:
            conn.close()
            print("✅ Connection closed")
        except:
            pass
        
        # 关闭服务器
        server.close()
        print("✅ Server closed")

if __name__ == "__main__":
    main()
