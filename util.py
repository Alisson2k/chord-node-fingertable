import service_pb2
import service_pb2_grpc

def treat_command(channel, comando, chave, valor, atual = False):
    stub = service_pb2_grpc.HashStub(channel)

    if comando == 'create':
        return stub.Create(service_pb2.HashRequest(comando=comando,
                                                   chave=chave,
                                                   valor=valor,
                                                   atual=atual))
    elif comando == 'read':
        return stub.Read(service_pb2.HashRequest(comando=comando,
                                                 chave=chave,
                                                 valor=valor,
                                                 atual=atual))
    elif comando == 'update':
        return stub.Update(service_pb2.HashRequest(comando=comando,
                                                   chave=chave,
                                                   valor=valor,
                                                   atual=atual))
    elif comando == 'delete':
        return stub.Delete(service_pb2.HashRequest(comando=comando,
                                                   chave=chave,
                                                   valor=valor,
                                                   atual=atual))
