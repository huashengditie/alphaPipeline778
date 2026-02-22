# import numpy as np
# import pandas as pd
# from pyworldquant.spot import Spot as Client
import requests
import json
from time import sleep
from os.path import expanduser
from requests.auth import HTTPBasicAuth
import logging

def sign_in(username='', password=''):
    try:
        sess = requests.Session()
        sess.auth = HTTPBasicAuth(username, password)
        response = sess.post("https://api.worldquantbrain.com/authentication")
        print(response.status_code)
        print(response.json())
        if response.status_code == 201:
            print("Login successful!")
            return sess
        else:
            print("Login failed.")
            return None
    except Exception as e:
        print(f"Error during sign-in: {e}")
        return None

sess=requests.Session()
sess.auth=HTTPBasicAuth(username,password)
response= sess.post("https://api.worldquantbrain.com/authentication")
print(response. status_code)
print(response.json())
logging.basicConfig(filename="simulation.log",level=logging.INFO,format="%(asctime)s-%(levelname)s-%(message)s")


def get_datafields(
        s,
        searchScope,
        dataset_id: str = '',
        search: str = ''
):
    import pandas as pd
    instrument_type = searchScope['instrumentType']
    region = searchScope['region']
    delay = searchScope['delay']
    universe = searchScope['universe']
    if len(search) == 0:
        url_template = "https://api.worldquantbrain.com/data-fields?" + \
                       f"&instrumentType={instrument_type}" + \
                       f"&region={region}&delay={str(delay)}&universe={universe}&dataset.id={dataset_id}&limit=50" + \
                       "&offset={x}"
        count = s.get(url_template.format(x=0)).json()['count']
    else:
        url_template = "https://api.worldquantbrain.com/data-fields?" + \
                       f"&instrumentType={instrument_type}" + \
                       f"&region={region}&delay={str(delay)}&universe={universe}&limit=50" + \
                       f"&search={search}" + \
                       "&offset={x}"
        count = 100
    datafields_list = []
    for x in range(0, count, 50):
        datafields = s.get(url_template.format(x=x))
        datafields_list.append(datafields.json()['results'])
    datafields_list_flat = [item for sublist in datafields_list for item in sublist]

    datafields_df = pd.DataFrame(datafields_list_flat)
    return datafields_df


# 两个datafield alpha list generation
def alpha_list_generation2(datafield_list, datafield_list2, type, region, delay, decay, universe, truncation):
    alpha_list = []
    for datafield in datafield_list:
        for datafield2 in datafield_list2:
            print("正在将如下Alpha表达式与setting封装")
            alpha_expression = f'{datafield}/{datafield2}'
            print(alpha_expression)
            simulation_data = {
                'type': 'REGULAR',
                'settings': {
                    'instrumentType': type,
                    'region': region,
                    'universe': universe,
                    'delay': delay,
                    'decay': decay,
                    'neutralization': 'SUBINDUSTRY',
                    'truncation': truncation,
                    'pasteurization': 'ON',
                    'unitHandling': 'VERIFY',
                    'nanHandling': 'ON',
                    'language': 'FASTEXPR',
                    'visualization': False,
                },
                'regular': alpha_expression
            }
            alpha_list.append(simulation_data)
    print(f'there are {len(alpha_list)} Alphas to simulate')
    return alpha_list

logging.basicConfig(filename='simulation.log',level=logging.INFO,format='%(asctime)s - %(levelname)s - %(message)s')
def testing_alphas(alpha_list):
    sess=sign_in()
    alpha_fail_attempt_tolerance = 3
    for idx, alpha in enumerate(alpha_list):
        failure_count = 0
        has_relogged = False
        while True:
            try:
                sim_resp = sess.post(
                    'https://api.worldquantbrain.com/simulations',
                    json=alpha
                )
                if 'Location' not in sim_resp.headers:
                    raise RuntimeError(
                        f"Submit failed | status={sim_resp.status_code} | "
                        f"response={sim_resp.text[:200]}"
                    )
                sim_progress_url = sim_resp.headers['Location']
                logging.info(f"[{idx}] Alpha submitted: {sim_progress_url}")
                print(f"[{idx}] Alpha submitted: {sim_progress_url}")

                # ---- POLL SIMULATION ----
                while True:
                    sim_progress_resp = sess.get(sim_progress_url)
                    retry_after = float(
                        sim_progress_resp.headers.get("Retry-After", 0)
                    )
                    if retry_after == 0:
                        break
                    sleep(retry_after)
                alpha_id = sim_progress_resp.json().get("alpha")
                print(f"Simulation complete. Alpha ID: {alpha_id}")
                logging.info(f"Simulation complete. Alpha ID: {alpha_id}")
                break  # ✅ success → next alpha
            except Exception as e:
                failure_count += 1
                logging.error(
                    f"Alpha error (attempt {failure_count}): {e}"
                )
                print(
                    f"Error (attempt {failure_count}/"
                    f"{alpha_fail_attempt_tolerance}): {e}"
                )
                sleep(5)
                if failure_count < alpha_fail_attempt_tolerance:
                    continue
                # ---- EXCEEDED TOLERANCE ----
                if not has_relogged:
                    logging.warning("Retry limit reached. Re-authenticating...")
                    print("Retry limit reached. Re-authenticating...")
                    try:
                        sess = sign_in()
                        has_relogged = True
                        failure_count = 0
                        continue
                    except Exception as login_err:
                        logging.error(f"Re-login failed: {login_err}")
                        print(f"Re-login failed: {login_err}")
                        break
                else:
                    msg = (
                        f"Skipping alpha after re-login failure: "
                        f"{alpha.get('regular', 'Unknown')}"
                    )
                    logging.error(msg)
                    print(msg)
                    break

searchScope = {'region': 'USA', 'delay': '1', 'universe': 'TOP3000', 'instrumentType': 'EQUITY'}
#Set 1
sentimentvolume_data= get_datafields(s=sess, searchScope=searchScope, dataset_id='pv1',search="income")
sentimentvolume_data = sentimentvolume_data[sentimentvolume_data["type"] == "MATRIX"]
datafield1=sentimentvolume_data["id"].values
#+
sentimentvolume2_data= get_datafields(s=sess, searchScope=searchScope, dataset_id='',search="equity")
sentimentvolume2_data = sentimentvolume2_data[sentimentvolume2_data["type"] == "MATRIX"]
datafield2=sentimentvolume2_data["id"].values
#testing_alphas(alpha_list1[2392:])
alpha_list2=alpha_list_generation2(datafield1,datafield2,"EQUITY","USA",1,1,"TOP3000",0.08)
testing_alphas(alpha_list2[107:])
# testing_alphas(alpha_list3)