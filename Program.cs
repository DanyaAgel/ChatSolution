using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace ChatServer
{
    class Program
    {
        static async Task Main(string[] args)
        {
            var server = new ChatServer();
            await server.Start();
        }
    }

    public class ChatServer
    {
        private TcpListener tcpListener;
        private List<TcpClient> clients = new List<TcpClient>();
        private Dictionary<TcpClient, string> clientNames = new Dictionary<TcpClient, string>();
        private bool isRunning = true;

        public async Task Start()
        {
            Console.WriteLine("=== TCP ЧАТ СЕРВЕР ===");

            // Ввод IP адреса сервера
            Console.Write("Введите IP адрес сервера (например, 127.0.0.1 или 192.168.1.100): ");
            string serverIp = Console.ReadLine();

            // Ввод порта для TCP
            Console.Write("Введите TCP порт для чата: ");
            int tcpPort = int.Parse(Console.ReadLine());

            // Проверка доступности порта
            if (!IsTcpPortAvailable(serverIp, tcpPort))
            {
                Console.WriteLine($"Ошибка: порт {tcpPort} уже используется!");
                return;
            }

            try
            {
                // Запуск TCP сервера на указанном IP
                IPAddress ipAddress = IPAddress.Parse(serverIp);
                tcpListener = new TcpListener(ipAddress, tcpPort);
                tcpListener.Start();
                Console.WriteLine($"TCP сервер запущен на {serverIp}:{tcpPort}");

                // Запуск обработки TCP подключений
                _ = Task.Run(AcceptClients);

                Console.WriteLine("Сервер запущен. Нажмите Enter для остановки...");
                Console.ReadLine();

                isRunning = false;
                Stop();
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Ошибка при запуске сервера: {ex.Message}");
            }
        }

        private bool IsTcpPortAvailable(string ip, int port)
        {
            try
            {
                TcpListener listener = new TcpListener(IPAddress.Parse(ip), port);
                listener.Start();
                listener.Stop();
                return true;
            }
            catch
            {
                return false;
            }
        }

        private async Task AcceptClients()
        {
            while (isRunning)
            {
                try
                {
                    var tcpClient = await tcpListener.AcceptTcpClientAsync();
                    lock (clients)
                    {
                        clients.Add(tcpClient);
                    }
                    _ = Task.Run(() => HandleClient(tcpClient));
                }
                catch (Exception ex)
                {
                    if (isRunning)
                        Console.WriteLine($"Ошибка при подключении клиента: {ex.Message}");
                }
            }
        }

        private async Task HandleClient(TcpClient tcpClient)
        {
            string clientName = "Неизвестный";
            string clientEndPoint = tcpClient.Client.RemoteEndPoint?.ToString() ?? "Unknown";

            try
            {
                NetworkStream stream = tcpClient.GetStream();
                byte[] buffer = new byte[4096];

                // Получаем имя клиента
                int bytesRead = await stream.ReadAsync(buffer, 0, buffer.Length);
                clientName = Encoding.UTF8.GetString(buffer, 0, bytesRead);

                lock (clientNames)
                {
                    clientNames[tcpClient] = clientName;
                }

                Console.WriteLine($"Клиент {clientName} подключился с адреса {clientEndPoint}");
                await BroadcastSystemMessage($"{clientName} присоединился к чату", "connect", clientName);

                // Обработка сообщений от клиента
                while (isRunning && tcpClient.Connected)
                {
                    bytesRead = await stream.ReadAsync(buffer, 0, buffer.Length);
                    if (bytesRead == 0) break;

                    string message = Encoding.UTF8.GetString(buffer, 0, bytesRead);
                    Console.WriteLine($"{clientName}: {message}");
                    await BroadcastMessage(message, clientName);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Ошибка при обработке клиента {clientName}: {ex.Message}");
            }
            finally
            {
                RemoveClient(tcpClient);
                await BroadcastSystemMessage($"{clientName} покинул чат", "disconnect", clientName);
            }
        }

        private async Task BroadcastMessage(string message, string senderName)
        {
            string formattedMessage = $"{senderName}: {message}";
            byte[] data = Encoding.UTF8.GetBytes($"MSG|{formattedMessage}");

            List<TcpClient> clientsCopy;
            lock (clients)
            {
                clientsCopy = new List<TcpClient>(clients);
            }

            foreach (var client in clientsCopy)
            {
                try
                {
                    if (client.Connected)
                    {
                        var stream = client.GetStream();
                        await stream.WriteAsync(data, 0, data.Length);
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Ошибка при отправке сообщения: {ex.Message}");
                }
            }
        }

        private async Task BroadcastSystemMessage(string message, string type, string clientName)
        {
            byte[] data = Encoding.UTF8.GetBytes($"SYS|{type}|{clientName}|{message}");

            List<TcpClient> clientsCopy;
            lock (clients)
            {
                clientsCopy = new List<TcpClient>(clients);
            }

            foreach (var client in clientsCopy)
            {
                try
                {
                    if (client.Connected)
                    {
                        var stream = client.GetStream();
                        await stream.WriteAsync(data, 0, data.Length);
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Ошибка при отправке системного сообщения: {ex.Message}");
                }
            }
        }

        private void RemoveClient(TcpClient client)
        {
            lock (clients)
            {
                clients.Remove(client);
            }
            lock (clientNames)
            {
                if (clientNames.ContainsKey(client))
                    clientNames.Remove(client);
            }
            client.Close();
        }

        private void Stop()
        {
            tcpListener?.Stop();

            lock (clients)
            {
                foreach (var client in clients)
                {
                    client.Close();
                }
                clients.Clear();
            }

            Console.WriteLine("Сервер остановлен");
        }
    }
}