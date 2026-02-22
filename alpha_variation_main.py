

import copy
import json
import os
import requests
from requests.auth import HTTPBasicAuth
from typing import List, Dict
from concurrent.futures import ThreadPoolExecutor, as_completed
import argparse
import logging
import time
from variable_list import generate_alpha_variants, element
from iteration_main import testing_alphas

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
class AlphaSubmitter:
    def __init__(self):
        self.sess = requests.Session()
        self.sess.auth = HTTPBasicAuth(username="ENTER UR USERNAME", password="ENTER YOUR PASSWORD")
        response = self.sess.post("https://api.worldquantbrain.com/authentication")
        if response.status_code != 201:
            raise Exception(f"Authentication failed: {response.text}")
        logger.info("Successfully logged into WorldQuant Brain")

    def fetch_successful_alphas(self, max_items: int = 180, page_size: int = 100) -> Dict:
        url = "https://api.worldquantbrain.com/users/self/alphas"
        max_retries = 3
        retry_delay = 60
        collected = []
        offset = 0
        while offset < max_items:
            limit = min(page_size, max_items - offset)
            params = {
                "limit": limit,
                "offset": offset,
                "status": "UNSUBMITTED",
                "order": "-dateCreated",
                "hidden": "false"
            }
            # 这里首先筛选出unsubmitted alpha and then locally filter out using the parameters we needed.
            logger.info(f"Fetching Alphas with params: {params}")
            for attempt in range(max_retries):
                try:
                    response = self.sess.get(url, params=params)
                    if response.status_code == 429:  # 如果达到了API的rate limit 就等一下
                        wait_time = int(response.headers.get("Retry-After", retry_delay))
                        logger.info(f"Rate limited. Waiting {wait_time} seconds...")
                        time.sleep(wait_time)
                        continue
                    response.raise_for_status()
                    data = response.json()
                    results = data.get("results", [])
                    if not results:
                        return {"count": len(collected), "results": collected}
                    filtered = [
                        alpha for alpha in results
                        if (alpha.get("is") or {}).get("sharpe", 0) >= 1.00
                           and (alpha.get("is") or {}).get("fitness", 0) >= 0.5
                    ]
                    collected.extend(filtered)
                    break
                except Exception as e:
                    logger.warning(f"Fetch failed (Attempt {attempt + 1}): {str(e)}")
                    time.sleep(retry_delay)
            else:
                break
            offset += limit
        return {"count": len(collected), "results": collected}
ok = AlphaSubmitter()
data = ok.fetch_successful_alphas()
print(f"alpha2_0 raw results count: {len(data.get('results', []))}")
print(f"alpha2_0 raw results: {data.get('results', [])}")
alpha2_0 = []
for alpha in data.get("results", []):
    regular = alpha.get("regular")
    if isinstance(regular, dict):
        alpha_code = regular.get("code")
    else:
        alpha_code = regular
    if alpha_code:
        alpha_id = alpha.get("id") or alpha.get("alphaId") or alpha.get("alpha") or alpha.get("name")
        print(f"{alpha_id}\t{alpha_code}")
        rendered = copy.deepcopy(element)
        rendered["regular"] = alpha_code
        alpha2_0.append(rendered)
print("alpha2_0 successful printed")

# 在这里可以手动插入一段alpha2.0让alpha3.0来处理：

alpha3_0 = generate_alpha_variants(alpha2_0)
print("alpha 3 长度为：")
print(len(alpha3_0))
print("ALPHA LIST3.0 SUCCESSFULLY GENERATED NOW TESTING ALPHA3.0")
with open("alpha3_0", "w", encoding="utf-8") as f:
    json.dump(alpha3_0, f, ensure_ascii=True, indent=2)
print("alpha3_0 written to file: alpha3_0")
testing_alphas(alpha3_0, ok.sess)
