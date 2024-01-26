import datetime

import requests
import concurrent.futures


def get_data_from_web(proxy: dict, url_: str, header_: dict = None) -> dict:
    try:

        response = requests.get(url=url_, headers=header_, proxies=proxy, timeout=15)

        return {"content": "successful", "data": response.json()}
    except Exception as e:
        return {"content": "fail", "proxy": f"{proxy} is NOT working", "error_message": str(e)}


def check_if_proxy_working_over_web(proxy: dict, sample_website_url: str = 'https://example.org/',
                                    timeout: int = 3) -> dict:
    try:
        response = requests.get(url=sample_website_url, proxies=proxy, timeout=timeout)
        return {"content": "successful", "proxy": f"{proxy} is working"}
    except Exception as e:
        return {"content": "fail", "proxy": f"{proxy} is NOT working", "error_message": str(e)}


def get_proxy_list() -> list[str]:
    proxy_list = []
    with open("proxies/proxy.txt") as f:
        for line in f:
            proxy_list.append(line.strip())
    return proxy_list


def get_working_proxies(proxy_item: str) -> str: # list[str]:


        proxy: dict = {
            "http": "http://" + proxy_item,
            "https": "http://" + proxy_item
        }
        response_result = check_if_proxy_working_over_web(proxy=proxy)
        if response_result["content"] == "successful":
            print(f"{proxy_item} - working")
            return proxy_item
        else:
            print(f"{proxy_item} - NOT working")



# prxy_list = get_proxy_list()
# with concurrent.futures.ThreadPoolExecutor() as executor:
#     executor.map(get_working_proxies, prxy_list)
# working_proxy: dict = {
#             "http": "http://" + '167.114.107.37:80',
#             "https": "http://" + '167.114.107.37:80'
#         }
#
# print(get_data_from_web(proxy=working_proxy, url_=nse_url, header_=header))

print(datetime.)
