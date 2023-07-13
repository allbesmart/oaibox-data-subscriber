import getpass
import json
import logging
import time
import requests
import os
from dotenv import load_dotenv
from keycloak import KeycloakOpenID
from classes.client import Client
import pandas as pd
import xarray as xr
import numpy as np

username = ""
password = ""

telemetry_data = []
subscribed_machine = int()
file_wanted = False

load_dotenv()
access_token_url = os.environ['access_token_url']
tenant_url = os.environ['tenant_api_url']
brokerUrl = os.environ['broker_url']

logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.DEBUG,
)


def request_credentials():
    """
    request_credentials()
    
    Prompts the console until user inputs username and password.
    
    Returns:
        list like [user, password]
    """
    global username
    global password
    
    while username == "":
        username = input("Insert username: ")
    
    while password == "":
        password = getpass.getpass("Insert password: ")
    
    return [username, password]


def get_token():
    """
    get_token()
    
    Makes an authorized request for a token, if unsuccessful requests credentials again.
    
    Returns:
        str: access token
    """
    
    user_credentials = request_credentials()
    
    keycloak = KeycloakOpenID(server_url=access_token_url, realm_name="OAIBOX", client_id="OAIBOX-API")
    
    token = keycloak.token(user_credentials[0], user_credentials[1])
    
    return token


def get_machines(token):
    """
    get_machines()
    
    Makes an authorized request for the user's tenants

    Returns:
        list(dict()): list with all machines the user has access to, dict contains id and oaiboxType
    """
    
    tenant_headers = {
        'Authorization': 'Bearer ' + token,
    }
    tenants = requests.get(tenant_url, headers=tenant_headers)
    machines = []
    i = 1
    for tenant in tenants.json()['availableTenants']:
        for machine in tenant['registeredMachines']:
            machine['tenantId'] = tenant['id']
            machines.append(machine)
            i += 1
    
    return machines
    
    
def subscription_callback(frame):
    telem = json.loads(frame.body)
    telemetry_data.append(telem)
    

def conn(token):
    
    global subscribed_machine
    
    tenant = subscribed_machine['tenantId']
    machine = subscribed_machine['id']
    
    headers = {
        'Authorization': 'Bearer ' + token['access_token'],
    }
    
    client = Client(brokerUrl)
    topic = '/topic/' + tenant + '.' + machine + '.gnb.telemetry'
    is_connected = False
    while not is_connected:
        print("Establishing STOMP connection...")
        is_connected = client.connect(headers=headers)
        time.sleep(0.5)
    
        
    client.subscribe(topic, callback=subscription_callback, headers=headers)
    
    try:
        while True:
            if client.connected:
                continue
    except KeyboardInterrupt:
        
        client.disconnect()
        
        telems_list = []
        timestamps = []
        for telem in telemetry_data:
            if telem['ues']:
                ue_list = []
                for ue in telem['ues']:
                    ue_list.append(ue)
                telems_list.append(ue_list)
                timestamps.append(telem['timestamp'])
        
        telem_df = pd.json_normalize(telems_list).set_axis(timestamps, axis='index')
        
        if file_wanted:
            telem_df.to_csv(os.getcwd() + "/output/" + str(int(time.time())) + "." + machine + ".csv")
            print("File saved as " + str(int(time.time())) + "." + machine + ".csv in data directory!")
        
        print('Job done.')
        
    except:
        client.disconnect()
    

def main():
    
    global file_wanted
    global subscribed_machine
    
    token = get_token()
    
    machine_list = get_machines(token['access_token'])
    
    print("You have the following machines available: ")
    for i, machine in enumerate(machine_list):
        print( str(i) + " with id " + machine['id'] + " and type " + machine['oaiboxType'])
        
    print("Select which machine you want to subscribe data from:")
    machine_to_subscribe = int(input())
    while machine_to_subscribe < 0 or machine_to_subscribe > len(machine_list) - 1:
        print("The number inserted does not match a machine, try again:")
        machine_to_subscribe = int(input())
    subscribed_machine = machine_list[machine_to_subscribe]
    
    print("Would you like to save the data to a file? (Y/N):")
    want_to_save = input().lower()
    while want_to_save != 'y' and want_to_save != 'n':
        print("Incorrect input type y/Y for Yes or n/N for no:")
        want_to_save = input().lower()
    file_wanted = True if want_to_save == 'y' else False
    
    conn(token) # TODO change using globals

if __name__ == "__main__":
    
    main()