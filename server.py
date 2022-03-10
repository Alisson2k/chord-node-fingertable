from concurrent import futures
import grpc
import service_pb2
import service_pb2_grpc
import math
import hashlib
import util

NODES = 8
MAX_KEYS = 64
MAX_GRPC_WORKERS = 10

def str_to_bits(key):
    return int.from_bytes(hashlib.sha256(key.encode()).digest()[:4], 'little') % MAX_KEYS

def geraFingerTable(id, servers, N):

    finger_table = dict()

    for i in range(int(MAX_KEYS)):
        tam_salto = 2**i
        
        if tam_salto > int(MAX_KEYS/2):
            break

        pos_salto = (id + tam_salto)%MAX_KEYS

        for i in range(N):
            if pos_salto <= servers[i].id:
                responsavel = servers[i]
                break

        finger_table[pos_salto] = responsavel

    return finger_table

class HashService(service_pb2_grpc.HashServicer):

    def __init__(self, node, send_to):
        self.node = node
        self.send_to = send_to

    def available_node(self, chave, atual):
        hash_key = str_to_bits(chave)

        if atual:
            return (-1, True)

        val = list(map(lambda x : x.id, self.node.fingertable.values()))

        escolha = 0
        maior_limite = True
 
        for id in val:
            if int(hash_key) == int(id):
                choice = id
                maior_limite = False
                break
            elif int(hash_key) < int(id):
                choice = val[escolha]
                maior_limite = False
                break

            escolha += 1

        if maior_limite:
            if val[escolha-1] < val[escolha-2]:
                choice = val[escolha-2]
            else:
                choice = val[escolha-1]

        print(f'[+] Chave: [{hash_key}], requisição transferida do nó [{self.node.id}] para o nó [{choice}]')

        return (choice, not maior_limite)

    def Create(self, request, context):
        (node_id, atual) = self.available_node(request.chave, request.atual)

        if request.atual or node_id == self.node.id:
            try:
                self.node.hash[request.chave] = request.valor
                return service_pb2.HashReply(codigo=0, resposta=f'Receita [{request.chave}] criada com sucesso')
            except Exception as e:
                return service_pb2.HashReply(codigo=1, resposta=f'ERRO: {str(e)}')
        else:
            return self.send_to(node_id, request.comando, request.chave, request.valor, atual)

    def Read(self, request, context):
        (node_id, atual) = self.available_node(request.chave, request.atual)
        
        if request.atual or node_id == self.node.id:
            try:
                valor = self.node.hash[request.chave]
                return service_pb2.HashReply(codigo=0, resposta=valor)
            except Exception as e:
                return service_pb2.HashReply(codigo=1, resposta=f'ERRO: {str(e)}')
        else:
            return self.send_to(node_id, request.comando, request.chave, request.valor, atual)

    def Update(self, request, context):
        (node_id, atual) = self.available_node(request.chave, request.atual)
        
        if request.atual or node_id == self.node.id:
            try:
                self.node.hash[request.chave] = request.valor
                return service_pb2.HashReply(codigo=0, resposta=f'Receita [{request.chave}] atualizada com sucesso')
            except Exception as e:
                return service_pb2.HashReply(codigo=1, resposta=f'ERRO: {str(e)}')
        else:
            return self.send_to(node_id, request.comando, request.chave, request.valor, atual)

    def Delete(self, request, context):
        (node_id, atual) = self.available_node(request.chave, request.atual)
        
        if request.atual or node_id == self.node.id:
            try:
                del self.node.hash[request.chave]
                return service_pb2.HashReply(codigo=0, resposta=f'Receita [{request.chave}] deletada com sucesso')
            except Exception as e:
                return service_pb2.HashReply(codigo=1, resposta=f'ERRO: {str(e)}')
        else:
            return self.send_to(node_id, request.comando, request.chave, request.valor, atual)

class Node(object):
    def __init__(self, ip, port, index, id, value_range, send_to):
        self.ip = ip
        self.port = port
        self.index = index
        self.id = id
        self.range = value_range
        self.send_to = send_to
        self.hash = dict()
        self.fingertable = dict()

    def run(self):
        self.server = grpc.server(futures.ThreadPoolExecutor(max_workers=MAX_GRPC_WORKERS))

        service_pb2_grpc.add_HashServicer_to_server(HashService(self, self.send_to), self.server)
        self.server.add_insecure_port(f'{self.ip}:{self.port}')
        self.server.start()

        print(f"[+] Host do servidor: [{self.ip}:{self.port}] conectado e pronto para receber chamadas!")

    def stop(self):
        self.server.stop(True)

    def __str__(self):
        return f'Node[ip={self.ip}, port={self.port}, id={self.id}, range={self.range}, next={self.next}]'

class GrpcServer(object):
    def __init__(self, N, qtd_chaves):
        self.size = N
        self.qtd_chaves = qtd_chaves
        self.range_size = math.ceil(qtd_chaves/N)
        self.servers = list()

        for i in range(0, N):
            ip = '127.0.0.1'
            port = 5050 + i

            # value_range = (
            #     (i * self.range_size)+1,
            #     min(((i + 1) * self.range_size) - 1, self.qtd_chaves - 2)
            # )
            if i == 0 :
                value_range = (
                    (i * self.range_size),((i + 1) * self.range_size) - 1
                )
            else:
                value_range = (
                    (i * self.range_size)+1,((i + 1) * self.range_size) - 1
                )

            self.servers.append(Node(ip, port, i, (i + 1) * self.range_size, value_range, self.send_to))

        for i in range(0, N-1):
            self.servers[i].next_index = i + 1
            self.servers[i].next = (i + 2) * self.range_size

        self.servers[N-1].next = self.servers[0].id
        
        for i in range(0, N):
            self.servers[i].fingertable = geraFingerTable(self.servers[i].id, self.servers, N)
            
            print(f'\nServer: {self.servers[i].id}\nFinger Table:')
            for key, val in self.servers[i].fingertable.items():
                print(f'{key} : {val.id}')

    def start_servers(self):
        print(f'[+] Iniciando os {self.size} servidores')
        try:
            for server in self.servers:
                print(f'[+] Iniciando {server}')
                server.run()

            print("\n[*] Caso deseje encerrar o processo digite: [stop]")
            while(True):
                x = input()
                if x == 'stop':
                    break
                pass

        except KeyboardInterrupt:
            print("\n[-] Forçando parada via terminal")
            for server in self.servers:
                server.stop()

    def send_to(self, node_id, comando, chave, valor, atual):
        cur_node = None
        for server in self.servers:
            if server.id == node_id:
                cur_node = server

        if cur_node is not None:
            with grpc.insecure_channel(f'{cur_node.ip}:{cur_node.port}') as channel:
                return util.treat_command(channel, comando, chave, valor, atual)

if __name__ == "__main__":
    server_ring = GrpcServer(NODES, MAX_KEYS)
    server_ring.start_servers()
