"""
The code is licensed under the MIT license.
This software comes with absolutely NO warranty.
Copyright (C) datawater 2024-2025
"""

#!/usr/bin/env python3

import requests
import time
import os
from multiprocessing import cpu_count
from multiprocessing.pool import ThreadPool
from hashlib import sha256

url_output_sha: list[tuple[str, str, str]] = []

def init():
    urls: list[str] = []
    output_files: list[str] = []
    sha256: dict[str, str] = {}

    f_syzygy_list = open("list_of_syzygy_urls.txt", "r")
    f_sha256 = open("sha256", "r")
    
    urls = f_syzygy_list.read().split("\n")
    
    urls.sort()

    sha256_list_read = f_sha256.read().split("\n")
    sha256_map_like: dict[str, tuple[str, str]] = {}

    for sha256_ in sha256_list_read:
        split = sha256_.split("  ")

        if len(split[1]) == 8 + 5:
            continue

        sha256_map_like[split[1]] = split

    path_existed = False

    try:
        os.mkdir("./syzygy")
    except FileExistsError:
        path_existed = True
        print("[INFO] syzygy path already exists")

    urls_clone = urls
    urls = []

    for url in urls_clone:
        filename = url.split("/")[-1]

        output_file = "./syzygy/" + filename
        
        if path_existed == False:
            output_files.append(output_file)
            continue
        
        # Don't download files that already exist
        if os.path.exists(output_file):
            sha256_map_like.pop(filename)
        else:
            sha256_map_like[filename][1] = url
            output_files.append(output_file)
            urls.append(url)

    for value in sha256_map_like.values():
        sha256[value[1]] = value[0]

    del urls_clone
    del sha256_map_like

    global url_output_sha
    for i, url in enumerate(urls):
        url_output_sha.append((url, output_files[i], sha256[url]))

def download_url(args: tuple[str, str, str]) -> tuple[str, float]:
    url, output, sha = args
    t0 = time.time()

    try:
        r = requests.get(url)
        
        if sha256(r.content).hexdigest() != sha:
            print(f"[ERROR] url: `{url}` file: `{output}` doesn't match hash")
            return (url, time.time() - t0)
        
        f = open(output, "wb")
        f.write(r.content)
        f.close()

        del r

    except Exception as e:
        print('[ERROR] Exception in download_url():', e)

    return (url, time.time() - t0)

def download_parallel(args):
    cpus = cpu_count()
    cpus_to_use = cpus // 3
    print(f"[INFO] Using {cpus_to_use} threads.")

    results = ThreadPool(cpus_to_use).imap_unordered(download_url, args)
    
    for result in results:
        print('url:', result[0], 'time (s):', result[1])

def main():
    init()
    
    download_parallel(url_output_sha)

if __name__ == "__main__":
    main()
