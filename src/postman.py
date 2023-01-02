import requests


class Postman:
    def __init__(self):
        pass

    @staticmethod
    def send_request(method: str,
                     url: str,
                     headers: dict = None,
                     json: dict = None,
                     timeout: int = 20,
                     http_proxy: str = None,
                     https_proxy: str = None,
                     status_code_check: int = None):

        if http_proxy is not None or https_proxy is not None:
            proxies = {
                "http": "http://" + http_proxy if http_proxy is not None else "https://" + https_proxy,
                "https": "https://" + https_proxy if https_proxy is not None else "http://" + http_proxy
            }
        else:
            proxies = None

        response = requests.request(
            method=method,
            url=url,
            json=json,
            headers=headers,
            proxies=proxies,
            timeout=timeout
        )

        if status_code_check is not None and response.status_code != status_code_check:
            raise Exception(f"URL {url} is not available: {response.status_code}\n")
        return response


if __name__ == "__main__":
    postman_obj = Postman()
    print(postman_obj.send_request("GET", "https://free-proxy-list.net/"))
