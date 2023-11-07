import requests, json
import argparse
import os, sys

BASE_URL = "https://cloud-api.yandex.net/v1/disk/public/resources?offset={offset}&public_key={url}&path={path}"

import hashlib
CHUNK_SIZE_BYTES = 4
def md5_from_file(fname, chunk_size = 4096):
    hash_md5 = hashlib.md5()
    file_size = os.stat(fname).st_size
    readed_size = 0
    print(f"{fname} checking checksum")
    with open(fname, "rb") as f:
        for chunk in iter(lambda: f.read(chunk_size), b""):
            print_progress(readed_size, file_size)
            hash_md5.update(chunk)
            readed_size += chunk_size
    print()
    return hash_md5.hexdigest()

def print_progress(current, total):
    def mb(v):
        return v / 1024 / 1024
    print(f"\r{round(mb(current), 2)} Mb / {round(mb(total), 2)} Mb ({round(current*100/total, 2)} %)", end="")

class SkipDownload(Exception):
    pass


FILE_LIMIT_SIZE = 2 * 1024 * 1024 * 1024 #2gb
class FileTooBig(SkipDownload):
    pass

SKIP_VALIDATE = False
class FileValidated(SkipDownload):
    pass

class File:
    def __init__(self, payload, saveto):
        self.name = payload.get("name", "")
        self.type = payload.get("type", "unknown")
        self.file = payload.get("file", "")
        self.md5 =  payload.get("md5", "")
        self.saveto = saveto
        self.path = f"./{self.saveto}/{self.name}"

    def download(self):
        if self.type == "unknown":
            print("Unknown file")
            return False

        try:
            self.validate_hash()
            self.download_this()
            self.create_hash()
        except FileValidated:
            print(f"File: {self.path} already downloaded, checksum success")
        except FileTooBig:
            print(f"File: {self.path} downloaded size, such big... skip")
        except Exception as de:
            print(f"Cannot download: {self.name}, err: {de}")

        return True
    
    def download_this(self, chunk_size = 1024):
        print(f"Start download: {self.path}")
        r = requests.get(self.file, stream=True)
        downloaded_length = 0
        total_length = int(r.headers.get('content-length'))

        if total_length > FILE_LIMIT_SIZE:
            raise FileTooBig()

        with open(self.path, "wb") as f:
            for chunk in r.iter_content(chunk_size=chunk_size):
                print_progress(downloaded_length, total_length)
                if chunk:
                    downloaded_length += chunk_size
                    f.write(chunk)
                    f.flush()
            print()

    def validate_hash(self):
        if os.path.exists(self.path):
            if os.path.exists(self.path + ".md5"):
                with open(self.path + ".md5", "r", encoding="utf8") as h:
                    hash_md5 = h.read().replace("\n","")
                    file_md5 = md5_from_file(self.path, chunk_size=CHUNK_SIZE_BYTES)
                    if hash_md5 == file_md5:
                        raise FileValidated()
                    else:
                        print(f"File: {self.path} already downloaded, but checksum is not equals! redownload")
            else:
                print(f"File: {self.path} founded, but checksum not found, redownload")

    def create_hash(self):
        print(f"File: {self.path} downloaded! Create hash file")
        with open(self.path + ".md5", "w", encoding="utf8") as h:
            h.write(self.md5)

class Directory:
    pass

class RequestsCache:
    cache = {}
    def __init__(self, cache_path = "./requests_cache.json"):
        self.path = cache_path
        if os.path.exists(self.path):
            print(f"Found cache: {self.path}")
            self.load()

    def load(self):
        with open(self.path, "r", encoding="utf-8") as cache:
            self.cache = json.loads(cache.read())

    def save(self):
        with open(self.path, "w", encoding="utf-8") as cache:
            cache.write(json.dumps(self.cache))

    def get(self, url):
        if url in self.cache:
            return self.cache[url]
        else:
            response = requests.get(url).json()
            self.cache[url] = response
            self.save()
            return self.cache[url]


class Downloader:
    url = ""
    def __init__(self, url):
        self.cache = RequestsCache(cache_path=f"./{url.split('https://disk.yandex.ru/d/')[-1].split('/')[0]}.json")
        self.url = url
        self.count = 0

    def download_loop(self):
        offset = 0
        limit = 20
        while True:
            print(f"Current offset: {offset}")
            has_one = False
            files = self.get_files(offset)
            self.count += files[1]
            for pf in files[0]:
                print(f"\rProcess: {offset}/~{self.count} ({round(offset*100/self.count, 2)}%)", end="")
                pf.download()
                offset += 1
                has_one = True
            if not has_one:
                print("Download ended")
                sys.exit(0)

    def get_files(self, offset, path = "/", before = ""):
        try:
            response = self.cache.get(BASE_URL.format(offset = offset, url = self.url, path = path))
            savepath = before + response['name'].replace(" ", "_")
            if not os.path.exists(savepath):
                print(f"Path {savepath} is not exists create him")
                os.mkdir(savepath)

            files = []
            files_len = 0

            for jf in response["_embedded"]["items"]:
                if jf.get("type", "unknown") == "file":
                    files.append(File(jf, savepath))
                    files_len += 1
                if jf.get("type", "unknown") == "dir":
                    print(f"Found directory: {jf['name']}")
                    inner_offset = 0
                    print(f"Search files in {jf['name']}, offset: {inner_offset}")
                    get_files = self.get_files(inner_offset, path = jf['path'], before = savepath + "/")
                    while get_files[1]>0:
                        files += get_files[0]
                        files_len += get_files[1]
                        inner_offset += get_files[1]
                        print(f"Search files in {jf['name']}, offset: {inner_offset}")
                        get_files = self.get_files(inner_offset, path = jf['path'], before = savepath + "/")
            return files, files_len
        except Exception as e:
            print(e)
            return [], 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("url", type=str, default="")
    parser.add_argument("--limit-size", type=int, default=2048)
    parser.add_argument("--validate-chunksize", type=int, default=4)
    args = parser.parse_args()
    FILE_LIMIT_SIZE = args.limit_size * 1024 * 1024
    CHUNK_SIZE_BYTES = args.validate_chunksize * 1024

    downloader = Downloader(args.url)
    try:
        downloader.download_loop()
    except KeyboardInterrupt:
        print("Ctrl+C pressed")
        sys.exit(0)