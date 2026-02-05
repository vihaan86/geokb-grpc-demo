import grpc
import geo_pb2
import geo_pb2_grpc


def ask(stub, question, top_k=10, include_sources=True):
    req = geo_pb2.GeoRequest(
        question=question,
        top_k=top_k,
        include_sources=include_sources,
    )
    resp = stub.Ask(req)
    print("\nQ:", question)
    print("A:", resp.answer)
    if resp.sources:
        print("Sources:")
        for s in resp.sources:
            print(" -", s)


def main():
    channel = grpc.insecure_channel("localhost:50051")
    stub = geo_pb2_grpc.GeoLLMServiceStub(channel)

    ask(stub, "capital of India")
    ask(stub, "Which continent is France in?")
    ask(stub, "list countries in Asia", top_k=5)
    ask(stub, "what is the capital of Brazil?")


if __name__ == "__main__":
    main()
