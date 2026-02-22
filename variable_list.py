import copy
import itertools
import json
import logging
import re
import time
from typing import Dict

import requests
from requests.auth import HTTPBasicAuth

group_list=["subindustry","market","sector","industry","country","currency"]
group_operator_list=["group_rank","group_scale","group_neutralize","group_zscore"]
ts_operator_list=["last_diff_value","ts_arg_max","ts_arg_min","ts_av_diff","ts_backfill","ts_corr","ts_count_nans","ts_decay_linear","ts_delay","ts_delta","ts_mean","ts_product","ts_quantile","ts_rank","ts_regression","ts_scale","ts_std_dev","ts_sum","ts_zscore"]

DEFAULT_USERNAME = ""
DEFAULT_PASSWORD = ""
logger = logging.getLogger(__name__)



def _extract_alpha_code(element) -> str:
    if isinstance(element, dict):
        return element.get("regular") or ""
    match = re.search(r"'regular'\s*:\s*'([^']*)'", element)
    if match:
        return match.group(1)
    match = re.search(r'"regular"\s*:\s*"([^"]*)"', element)
    if match:
        return match.group(1)
    return ""

def _token_present(alpha_code: str, token: str) -> bool:
    return re.search(rf"(?<!\w){re.escape(token)}(?!\w)", alpha_code) is not None

def _replace_token(alpha_code: str, token: str, replacement: str) -> str:
    return re.sub(rf"(?<!\w){re.escape(token)}(?!\w)", replacement, alpha_code)

def _number_dimensions(alpha_code: str):
    matches = list(re.finditer(r"\b\d+\b", alpha_code))
    if not matches:
        return alpha_code, []
    parts = []
    last = 0
    dimensions = []
    for idx, match in enumerate(matches):
        parts.append(alpha_code[last:match.start()])
        num_str = match.group(0)
        placeholder = f"__NUM{idx}__"
        parts.append(placeholder)
        last = match.end()
        if len(num_str) == 1:
            options = [num_str] + [str(v) for v in range(2, 12, 2) if str(v) != num_str]
        elif len(num_str) == 2:
            options = [num_str] + [str(v) for v in range(10, 50, 5) if str(v) != num_str]
        else:
            options = [num_str]
        dimensions.append((placeholder, options, num_str))
    parts.append(alpha_code[last:])
    return "".join(parts), dimensions

def generate_alpha_variants(alpha2_0,group_list=group_list, group_operator_list=group_operator_list,ts_operator_list=ts_operator_list):
    alpha3_0 = []
    seen = set()
    for element in alpha2_0:
        alpha_code = _extract_alpha_code(element)
        if not alpha_code:
            continue
        alpha_code_template, number_dimensions = _number_dimensions(alpha_code)
        token_dimensions = []
        for token in group_list:
            if _token_present(alpha_code, token):
                options = [token] + [v for v in group_list if v != token]
                token_dimensions.append((token, options, token))
        for token in group_operator_list:
            if _token_present(alpha_code, token):
                options = [token] + [v for v in group_operator_list if v != token]
                token_dimensions.append((token, options, token))
        for token in ts_operator_list:
            if _token_present(alpha_code, token):
                options = [token] + [v for v in ts_operator_list if v != token]
                token_dimensions.append((token, options, token))
        all_dimensions = token_dimensions + number_dimensions
        if not all_dimensions:
            continue
        base_tokens = [token for token, _, _ in all_dimensions]
        option_lists = [options for _, options, _ in all_dimensions]
        base_values = [original for _, _, original in all_dimensions]
        for combo in itertools.product(*option_lists):
            if all(chosen == original for chosen, original in zip(combo, base_values)):
                continue
            new_code = alpha_code_template
            for token, replacement in zip(base_tokens, combo):
                if token.startswith("__NUM"):
                    new_code = new_code.replace(token, replacement)
                else:
                    new_code = _replace_token(new_code, token, replacement)
            if isinstance(element, dict):
                new_element = copy.deepcopy(element)
                new_element["regular"] = new_code
                dedupe_key = json.dumps(new_element, sort_keys=True)
            else:
                new_element = element.replace(alpha_code, new_code, 1)
                dedupe_key = new_element
            if dedupe_key not in seen:
                seen.add(dedupe_key)
                alpha3_0.append(new_element)

    return alpha3_0

element={'type': 'REGULAR', 'settings': {'instrumentType': 'EQUITY', 'region': 'USA', 'universe': 'TOP3000', 'delay': 1, 'decay': 1, 'neutralization': 'SUBINDUSTRY', 'truncation': 0.01, 'pasteurization': 'ON', 'unitHandling': 'VERIFY', 'nanHandling': 'ON', 'language': 'FASTEXPR', 'visualization': False}, 'regular': 'XXXXX'}

class AlphaSubmitter:
    def __init__(self):
        self.sess = sign_in()

    def fetch_successful_alphas(self, max_items: int = 1000, page_size: int = 100) -> Dict:
        """Fetches up to the most recent max_items unsubmitted alphas and filters by metrics."""
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
                # Retries exhausted for this page.
                break

            offset += limit

        return {"count": len(collected), "results": collected}

