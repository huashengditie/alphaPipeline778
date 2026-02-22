import argparse
import json
import logging
import os
import time

import requests
from requests.auth import HTTPBasicAuth

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)
sess = None

# Optional hardcoded credentials (leave as None if not used).
HARDCODED_USERNAME = ""
HARDCODED_PASSWORD = ""


def sign_in(credentials_path=None, username=None, password=None):
    """Authenticate and return a session. Uses JSON credentials or env/args."""
    global sess
    if username is None or password is None:
        if credentials_path is None:
            username = HARDCODED_USERNAME or os.getenv("WQB_USERNAME")
            password = HARDCODED_PASSWORD or os.getenv("WQB_PASSWORD")
        else:
            with open(credentials_path, "r") as f:
                credentials = json.load(f)
            if isinstance(credentials, list):
                username = credentials[0].get("username")
                password = credentials[0].get("password")
            elif isinstance(credentials, dict):
                username = credentials.get("username")
                password = credentials.get("password")
            else:
                raise Exception("Invalid credentials format.")
    if not username or not password:
        raise Exception("Missing username or password for authentication. Set env WQB_USERNAME/WQB_PASSWORD or pass credentials.")

    sess = requests.Session()
    sess.auth = HTTPBasicAuth(username, password)
    try:
        response = sess.post("https://api.worldquantbrain.com/authentication")
        if response.status_code != 201:
            raise Exception(f"Authentication failed: {response.text}")
        logger.info("Successfully logged into WorldQuant Brain")
        return sess
    except Exception as e:
        logger.error(f"登录失败，错误信息: {str(e)}")
        return None


def setup_session(credentials_path="./credential.txt", username=None, password=None):
    """Initialize global session using credentials file or explicit username/password."""
    return sign_in(credentials_path=credentials_path, username=username, password=password)
def monitor_submission(alpha_id, max_attempts=30, sleep_time=10):
    url = f"https://api.worldquantbrain.com/alphas/{alpha_id}/submit"
    for attempt in range(max_attempts):
        try:
            response = sess.get(url)
            logger.info(f"监控尝试 {attempt + 1} alpha {alpha_id}")
            logger.info(f"响应状态: {response.status_code}")
            logger.info(f"响应内容: {response.text[:1000] if response.text else '无内容'}")

            if response.status_code != 200:
                logger.error(f"alpha {alpha_id} 提交可能失败")
                logger.error(f"响应状态: {response.status_code}")
                logger.error(f"响应文本: {response.text}")
                return {"status": "failed", "error": response.text}

            if not response.text.strip():
                logger.info(f"alpha {alpha_id} 仍在提交中，等待...")
                time.sleep(sleep_time)
                continue

            try:
                data = response.json()
                logger.info(f"alpha {alpha_id} 提交完成")
                return data
            except Exception as e:
                logger.warning(f"监控尝试 {attempt + 1} 解析失败: {str(e)}")
                logger.warning(f"响应内容: {response.text if 'response' in locals() else 'N/A'}")

        except Exception as e:
            logger.warning(f"监控请求尝试 {attempt + 1} 失败: {str(e)}")

        time.sleep(sleep_time)

    logger.error(f"alpha {alpha_id} 监控超时")
    return {"status": "timeout", "error": "监控超时"}


def log_submission_result(alpha_id, result):
    log_file = "submission_results.json"
    existing_results = []

    if os.path.exists(log_file):
        try:
            with open(log_file, 'r') as f:
                existing_results = json.load(f)
        except json.JSONDecodeError:
            logger.warning(f"无法解析 {log_file}，重置结果列表")
            existing_results = []

    entry = {
        "alpha_id": alpha_id,
        "timestamp": int(time.time()),
        "result": result
    }
    existing_results.append(entry)

    with open(log_file, 'w') as f:
        json.dump(existing_results, f, indent=2)
    logger.info(f"已记录alpha {alpha_id} 的提交结果")


def has_fail_checks(alpha):
    checks = alpha.get("result", {}).get("checks", [])
    return any(check.get("result") == "FAIL" for check in checks)


def submission_passed(result):
    if not result or not isinstance(result, dict):
        return False
    if result.get("status") == "failed":
        return False
    checks = result.get("is", {}).get("checks", [])
    if any(check.get("result") == "FAIL" for check in checks):
        return False
    return True


def submit_alpha(alpha_id):
    url = f"https://api.worldquantbrain.com/alphas/{alpha_id}/submit"
    logger.info(f"正在提交alpha {alpha_id}")
    logger.info(f"请求URL: {url}")

    try:
        response = sess.post(url)
        logger.info(f"响应状态: {response.status_code}")

        if response.status_code == 201:
            logger.info(f"成功提交alpha {alpha_id}，监控状态中...")
            result = monitor_submission(alpha_id)
            if result:
                log_submission_result(alpha_id, result)
                if submission_passed(result):
                    return True
                logger.error(f"alpha {alpha_id} 未通过检查，视为提交失败")
                return False
            else:
                logger.error(f"alpha {alpha_id} 提交监控超时")
                return False
        else:
            logger.error(f"提交alpha {alpha_id} 失败，状态: {response.status_code}")
            logger.error(f"响应文本: {response.text}")
            return False
    except Exception as e:
        logger.error(f"提交alpha {alpha_id} 时出错: {str(e)}")
        logger.exception("完整报错跟踪:")
        return False


def build_alpha_filter(min_sharpe=1.0, min_fitness=0.5, min_return=0.0):
    def _alpha_filter(alpha):
        metrics = alpha.get("is") or {}
        sharpe_val = metrics.get("sharpe") if metrics.get("sharpe") is not None else 0
        fitness_val = metrics.get("fitness") if metrics.get("fitness") is not None else 0
        return_val = metrics.get("return") if metrics.get("return") is not None else 0
        sharpe_ok = sharpe_val >= min_sharpe
        fitness_ok = fitness_val >= min_fitness
        ret_ok = return_val >= min_return
        return sharpe_ok and fitness_ok and ret_ok
    return _alpha_filter


def submit_filtered_alphas(
    max_items=3000,
    page_size=100,
    batch_size=5,
    min_sharpe=1.25,
    min_fitness=1.0,
    filter_fn=None,
    retry_delay=60,
):
    """
    Fetch unsubmitted alphas, filter by parameter conditions, and submit via backend API.
    Expects global `sess` and `logger` to be defined in this module.
    """
    if sess is None:
        raise Exception("Session not initialized. Call sign_in() first.")
    url = "https://api.worldquantbrain.com/users/self/alphas"
    offset = 0
    total_submitted = 0

    # Submitter ignores return threshold by default.
    active_filter = filter_fn or build_alpha_filter(
        min_sharpe=min_sharpe,
        min_fitness=min_fitness,
        min_return=0.0,
    )

    while offset < max_items:
        limit = min(page_size, max_items - offset)
        params = {
            "limit": limit,
            "offset": offset,
            "status": "UNSUBMITTED",
            "order": "-dateCreated",
            "hidden": "false",
        }
        logger.info(f"Fetching alphas with params: {params}")

        try:
            response = sess.get(url, params=params)
            if response.status_code == 429:
                wait_time = int(response.headers.get("Retry-After", retry_delay))
                logger.info(f"Rate limited. Waiting {wait_time} seconds...")
                time.sleep(wait_time)
                continue
            response.raise_for_status()
        except Exception as e:
            logger.warning(f"Fetch failed at offset {offset}: {str(e)}")
            time.sleep(retry_delay)
            continue

        data = response.json()
        results = data.get("results", [])
        if not results:
            logger.info("No more alphas returned by server")
            break

        filtered = [alpha for alpha in results if active_filter(alpha)]
        logger.info(f"Filtered {len(filtered)} alphas from {len(results)} candidates")

        for alpha in filtered[:batch_size]:
            alpha_id = alpha.get("id") or alpha.get("alphaId") or alpha.get("alpha") or alpha.get("name")
            if not alpha_id:
                continue
            if submit_alpha(alpha_id):
                total_submitted += 1

        offset += limit

    logger.info(f"Finished submitting. Total successful submits: {total_submitted}")
    return total_submitted


def batch_submit(batch_size=5):
    logger.info(f"开始批量提交，批次大小: {batch_size}")
    offset = 0
    total_submitted = 0

    while True:
        logger.info(f"获取偏移量 {offset} 的批次数据")
        # 【非原代码，补充缺失行】原截图未显示fetch_successful_alphas的调用参数完整写法
        response = fetch_successful_alphas(offset=offset, limit=batch_size)

        if not response or not response.get("results"):
            logger.info("没有更多alpha需要处理")
            break
        results = response["results"]
        if not results:
            logger.info("当前批次无alpha数据，终止提交")
            break

        # 【非原代码，补充缺失行】原截图此处缺失遍历results并调用submit_alpha的核心循环
        for alpha in results:
            alpha_id = alpha.get("id")
            if alpha_id:
                submit_success = submit_alpha(alpha_id)
                if submit_success:
                    total_submitted += 1

        offset += batch_size

    logger.info(f"批量提交完成，共成功提交 {total_submitted} 个alpha")


def main():
    parser = argparse.ArgumentParser(description="将成功的alpha提交到WorldQuant Brain")
    parser.add_argument("--credentials", type=str, default=None,
                        help="凭据文件路径（默认：None，使用环境变量）")
    parser.add_argument("--username", type=str, default=None,
                        help="用户名（可选，或用环境变量WQB_USERNAME）")
    parser.add_argument("--password", type=str, default=None,
                        help="密码（可选，或用环境变量WQB_PASSWORD）")
    parser.add_argument("--batch-size", type=int, default=5,
                        help="每批提交的alpha数量（默认：5）")
    parser.add_argument("--max-items", type=int, default=3000,
                        help="最多处理的alpha数量（默认：3000）")
    parser.add_argument("--min-sharpe", type=float, default=1.25,
                        help="Sharpe阈值（默认：1.25）")
    parser.add_argument("--min-fitness", type=float, default=1.0,
                        help="Fitness阈值（默认：1.0）")
    parser.add_argument("--log-level", type=str, default="INFO",
                        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
                        help="设置日志级别（默认：INFO）")
    args = parser.parse_args()

    logging.getLogger().setLevel(getattr(logging, args.log_level))
    sign_in(credentials_path=args.credentials, username=args.username, password=args.password)
    submit_filtered_alphas(
        max_items=args.max_items,
        batch_size=args.batch_size,
        min_sharpe=args.min_sharpe,
        min_fitness=args.min_fitness,
    )
if __name__ == "__main__":
    main()
