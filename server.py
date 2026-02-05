import csv
import re
import grpc
from concurrent import futures

import geo_pb2
import geo_pb2_grpc


def load_geo(csv_path: str):
    """
    Loads geo facts from CSV: Country, Capital, Continent
    Returns:
      countries: dict[country_lower] -> {"country":..., "capital":..., "continent":..., "row":...}
      by_continent: dict[continent_lower] -> list of same dict objects
    """
    countries = {}
    by_continent = {}

    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        for row in reader:
            if len(row) < 3:
                continue
            country = row[0].strip()
            capital = row[1].strip()
            continent = row[2].strip()

            key = country.lower()
            item = {
                "country": country,
                "capital": capital,
                "continent": continent,
                "row": ",".join([country, capital, continent]),
            }
            countries[key] = item

            ckey = continent.lower()
            by_continent.setdefault(ckey, []).append(item)

    return countries, by_continent


def normalize_spaces(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())


class GeoLLM(geo_pb2_grpc.GeoLLMServiceServicer):
    def __init__(self, csv_path: str = "geo.csv"):
        self.countries, self.by_continent = load_geo(csv_path)

    def Ask(self, request, context):
        q = normalize_spaces(request.question).lower()
        top_k = request.top_k if request.top_k > 0 else 10
        include_sources = bool(request.include_sources)

        sources = []

        # Pattern 1: "capital of <country>"
        m = re.search(r"\bcapital of (.+)$", q)
        if m:
            country_name = m.group(1).strip()
            item = self.countries.get(country_name.lower())
            if item:
                if include_sources:
                    sources.append(item["row"])
                return geo_pb2.GeoResponse(
                    answer=f"The capital of {item['country']} is {item['capital']}.",
                    sources=sources,
                )
            return geo_pb2.GeoResponse(
                answer=f"I don't have data for '{country_name}'.",
                sources=[],
            )

        # Pattern 2: "which continent is <country> in"
        m = re.search(r"\bwhich continent is (.+?) in\??$", q)
        if m:
            country_name = m.group(1).strip()
            item = self.countries.get(country_name.lower())
            if item:
                if include_sources:
                    sources.append(item["row"])
                return geo_pb2.GeoResponse(
                    answer=f"{item['country']} is in {item['continent']}.",
                    sources=sources,
                )
            return geo_pb2.GeoResponse(
                answer=f"I don't have data for '{country_name}'.",
                sources=[],
            )

        # Pattern 3: "list countries in <continent>"
        m = re.search(r"\blist countries in (.+)$", q)
        if m:
            cont = m.group(1).strip().lower()
            items = self.by_continent.get(cont, [])
            items = items[:top_k]

            if not items:
                return geo_pb2.GeoResponse(
                    answer=f"I don't have any countries for continent '{m.group(1).strip()}'.",
                    sources=[],
                )

            names = ", ".join([x["country"] for x in items])
            if include_sources:
                sources.extend([x["row"] for x in items])

            return geo_pb2.GeoResponse(
                answer=f"Countries in {m.group(1).strip()}: {names}.",
                sources=sources,
            )

        # Pattern 4: "capital of" but phrased as "what is the capital of <country>"
        m = re.search(r"\bwhat is the capital of (.+)\??$", q)
        if m:
            country_name = m.group(1).strip()
            item = self.countries.get(country_name.lower())
            if item:
                if include_sources:
                    sources.append(item["row"])
                return geo_pb2.GeoResponse(
                    answer=f"The capital of {item['country']} is {item['capital']}.",
                    sources=sources,
                )
            return geo_pb2.GeoResponse(
                answer=f"I don't have data for '{country_name}'.",
                sources=[],
            )

        # Fallback
        return geo_pb2.GeoResponse(
            answer=(
                "I can answer:\n"
                "1) 'capital of <country>'\n"
                "2) 'which continent is <country> in'\n"
                "3) 'list countries in <continent>'"
            ),
            sources=[],
        )


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    geo_pb2_grpc.add_GeoLLMServiceServicer_to_server(GeoLLM("geo.csv"), server)

    server.add_insecure_port("[::]:50051")
    server.start()
    print("✅ GeoLLM gRPC server running on port 50051")
    server.wait_for_termination()


if __name__ == "__main__":
    serve()
