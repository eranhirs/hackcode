from collections import namedtuple
from ortools.algorithms import pywrapknapsack_solver

files = [
    #"data/me_at_the_zoo.in",
    #"data/videos_worth_spreading.in",
    "data/trending_today.in",
    "data/kittens.in"
]

for file_name in files:

    with open(file_name, 'r') as f:
        lines = f.readlines()

    first_line = lines[0].split()

    solver = pywrapknapsack_solver.KnapsackSolver(
        pywrapknapsack_solver.KnapsackSolver.
            KNAPSACK_DYNAMIC_PROGRAMMING_SOLVER,
        'test')

    number_of_videos = int(first_line[0])
    number_of_endpoints = int(first_line[1])
    number_of_request_descriptions = int(first_line[2])
    number_of_cache_servers = int(first_line[3])
    capacity_of_cache_server = int(first_line[4])
    print "number_of_videos {} ; number_of_endpoints {} ; number_of_request_descriptions {} ; number_of_cache_servers {} ; capacity_of_cache_server {}".format(number_of_videos, number_of_endpoints, number_of_request_descriptions, number_of_cache_servers, capacity_of_cache_server)

    CacheServer = namedtuple("CacheServer", "id, videos, capacity")
    class Video:
        def __init__(self, id, size):
            self.id = id
            self.size = size
            self.weights = {}
    Endpoint = namedtuple("Endpoint", "id, latency, num_of_caches, cache_latencies")
    CacheLatency = namedtuple("CacheLatency", "cache_id, latency")
    Request = namedtuple("Request", "id, video_id, endpoint_id, num_of_requests")

    videos = {}
    video_sizes = []
    video_sizes_raw = lines[1].split()
    for video_id in range(0, len(video_sizes_raw)):
        video_size = int(video_sizes_raw[video_id])
        video = videos.get(video_id, Video(video_id, video_size))
        videos[video_id] = video

    cache_servers = {}
    cache_server_videos = []
    cache_servers_capacities = []

    line_number = 2
    endpoints = {}
    for endpoint_id in range(0, number_of_endpoints):
        endpoint_details = lines[line_number].split(" ")
        line_number += 1
        endpoint_latency = int(endpoint_details[0])
        endpoint_num_of_caches = int(endpoint_details[1].strip())
        endpoint_cache_latencies = {}

        for i in range(0, endpoint_num_of_caches):
            cache_details = lines[line_number].split(" ")
            line_number += 1
            cache_id = int(cache_details[0])
            cache_latency = int(cache_details[1].strip())
            endpoint_cache_latencies[cache_id] = CacheLatency(cache_id, cache_latency)

        endpoints[endpoint_id] = Endpoint(endpoint_id, endpoint_latency, endpoint_num_of_caches, endpoint_cache_latencies)

    requests = {}
    for request_id in range(0, number_of_request_descriptions):
        request_details = lines[line_number].split(" ")
        line_number += 1
        video_id = int(request_details[0])
        endpoint_id = int(request_details[1])
        num_of_requests = int(request_details[2].strip())
        requests[request_id] = Request(request_id, video_id, endpoint_id, num_of_requests)

    requests_by_endpoints = {}
    for request in requests.values():
        requests_by_endpoint = requests_by_endpoints.setdefault(request.endpoint_id, [])
        requests_by_endpoint.append(request)

    def remove_duplicate_videos(relevant_videos):
        new_relevant_videos = []
        new_relevant_videos_keys = set()
        for video, request in relevant_videos:
            if video.id not in new_relevant_videos_keys:
                new_relevant_videos.append((video, request))
                new_relevant_videos_keys.add(video.id)

        return new_relevant_videos

    def remove_low_value_videos(cache_server_id, relevant_videos):
        new_relevant_videos = []
        for video, request in relevant_videos:
            video_size = video.size
            new_relevant_videos.append(((video, request), int(float(video.weights[cache_server_id]) / video_size)))

        sorted_new_relevant_videos = sorted(new_relevant_videos, key=lambda item: item[1])
        sorted_new_relevant_videos = [tuple for (tuple, video_weight) in sorted_new_relevant_videos]

        return sorted_new_relevant_videos[:500]

    def calc_video_weight(cache_latency, request):
        endpoint_for_request = endpoints[request.endpoint_id]
        return request.num_of_requests * (endpoint_for_request.latency - cache_latency.latency)

    for cache_server_id in range(0, number_of_cache_servers):
        print("Starting cache_server_id", cache_server_id)
        cache_server = cache_servers.get(cache_server_id, CacheServer(cache_server_id, [], 0))
        relevant_endpoints = []
        for endpoint in endpoints.values():
            for cache_latency in endpoint.cache_latencies.values():
                if cache_latency.cache_id == cache_server_id:
                    relevant_endpoints.append((endpoint, cache_latency))

        relevant_videos = []
        relevant_videos_ids = set()
        for (endpoint, cache_latency) in relevant_endpoints:
            for request in requests_by_endpoints[endpoint.id]:
                video = videos[request.video_id]

                if request.video_id not in relevant_videos_ids:
                    relevant_videos.append((video, request))
                    relevant_videos_ids.add(request.video_id)
                    video.weights[cache_server_id] = calc_video_weight(cache_latency, request)
                else:
                    video.weights[cache_server_id] += calc_video_weight(cache_latency, request)

        print("Finished calculating relevant_videos cache_server_id", cache_server_id)

        print("Starting remover", cache_server_id)
        relevant_videos = remove_low_value_videos(cache_server_id, relevant_videos)
        videos_sizes = [video.size for video, request in relevant_videos]

        sizes = videos_sizes
        weights = []
        for video, request in relevant_videos:
            weights.append(video.weights[cache_server_id])

        print("Relevant videos number: ", len([video.size for video, request in relevant_videos]))

        weights_google = [sizes]
        capacities = [capacity_of_cache_server]
        values_google = weights
        solver.Init(values_google, weights_google, capacities)
        computed_value = solver.Solve()

        packed_items = [x for x in range(0, len(weights_google[0])) if solver.BestSolutionContains(x)]
        packed_weights = [weights_google[0][i] for i in packed_items]

        print("Packed items: ", packed_items)
        print("Packed weights: ", packed_weights)
        print("Total weight (same as total value): ", computed_value)


        #(min_target_function, videos_indexes) = knapsack.knapsack(sizes, weights).solve(capacity_of_cache_server)
        videos_for_cache_server = [relevant_videos[video_index] for video_index in packed_items]
        cache_servers[cache_server_id] = CacheServer(cache_server_id, videos_for_cache_server, 0)
        print "Finished cache server {} ; min_target_function {}".format(cache_server_id, computed_value)

    with open(file_name + '.out', 'w') as output:
        output.write(str(number_of_cache_servers))
        output.write('\n')

        for cache_server in cache_servers.values():
            video_id_str = ""
            for video, request in cache_server.videos:
                video_id_str += " " + str(video.id)
            output.write('{}{}\n'.format(str(cache_server.id), video_id_str))
