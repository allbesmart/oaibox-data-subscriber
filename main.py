import maskpass
import json
import threading
import time
from threading import Thread

import requests
import os
from dotenv import load_dotenv
from keycloak import KeycloakOpenID
from classes.client import Client
import pandas as pd
import matplotlib
import matplotlib.pyplot as plt
import matplotlib.animation as animation


class SubscriberData:
    """
    SubscriberData

    Class responsible for storing the data output by the Subscriber.
    """

    def __init__(self):
        """
        SubscriberData constructor.
        """
        self.data = []


class LivePlot(threading.Thread):
    """
    LivePlot()

    Class responsible for handling data plotting in a separate thread.
    """

    def __init__(self, sub_data):
        """
        LivePlot constructor.

        Args:
            sub_data: object of type SubscriberData
        """
        Thread.__init__(self)

        self.sub_data = sub_data
        self.fig = plt.figure()
        self.axis = self.fig.add_subplot(1, 1, 1)
        self.axis.get_xaxis().set_visible(False)
        self.anim = animation.FuncAnimation(self.fig, self.run_animation, frames=100, interval=1000)
        plt.show()

    def run_animation(self, i):
        """
        Function that updates the data to be plotted.

        Args:
            i: current frame, required but not used.
        """
        xs = []
        ys = []
        if self.sub_data.data:
            for telem in self.sub_data.data:
                ues = telem['ues']
                while len(ues) > len(xs):
                    xs.append([])
                    ys.append([])
                self.axis.clear()
                for i in range(len(ues)):
                    xs[i].append(int(telem['timestamp']))
                    ys[i].append(int(ues[i]['rsrp']))
                    self.axis.plot(xs[i], ys[i], label="rnti:" + ues[i]['rnti'])
            plt.legend()


class Subscriber(threading.Thread):
    """
        Subscriber

        Class responsible for handling the subscription in a separate thread.

        User will be prompted to input their credentials and select which machine is to be subscribed.
        User will be prompted whether a file is wanted, if 'Y', a file will be created upon terminating program, if 'N'
        the program will open a window displaying the rsrp values for the UEs of the subscribed machine.
    """

    def __init__(self, sub_data):
        """
        Subscriber constructor

        Args:
            sub_data: object of type SubscriberData
        """
        Thread.__init__(self)

        load_dotenv()
        self.access_token_url = os.environ['access_token_url']
        self.tenant_url = os.environ['tenant_api_url']
        self.brokerUrl = os.environ['broker_url']

        self.subscribed_machine = None
        self.client = None
        self.sub_data = sub_data
        self.connected = False
        self.file_wanted = False
        self.graph_wanted = False

    def request_credentials(self):
        """
        Prompts the console until user inputs username and password.

        Returns:
            list like [user, password]
        """
        username = ""
        password = ""

        while username == "":
            username = input("Insert username: ")

        while password == "":
            password = maskpass.askpass(prompt="Insert password: ", mask="*")

        return [username, password]

    def get_token(self):
        """
        Makes an authorized request for a token, if unsuccessful requests credentials again.

        Returns:
            str: access token
        """
        user_credentials = self.request_credentials()
        keycloak = KeycloakOpenID(server_url=self.access_token_url, realm_name="OAIBOX", client_id="OAIBOX-API")
        try:
            token = keycloak.token(user_credentials[0], user_credentials[1])
        except:
            print("You have entered the wrong credentials, try again!")
            token = self.get_token()
        return token

    def get_machines(self, token):
        """
        Makes an authorized request for the user's tenants.

        Args:
            token: object containing authorization token.

        Returns:
            list(dict()): list with all machines the user has access to, dict contains id and oaiboxType.
        """
        tenant_headers = {
            'Authorization': 'Bearer ' + token,
        }
        tenants = requests.get(self.tenant_url, headers=tenant_headers)
        machines = []
        i = 1
        for tenant in tenants.json()['availableTenants']:
            for machine in tenant['registeredMachines']:
                machine['tenantId'] = tenant['id']
                machine['clientDescription'] = tenant['clientDescription']
                machines.append(machine)
                i += 1
        return machines

    def subscription_callback(self, frame):
        """
        Callback for the subscription to the stomp connection.

        Args:
            frame: object containing message output from the subscription to the message broker.
        """
        telem = json.loads(frame.body)
        self.sub_data.data.append(telem)
        print('.', end='')

    def conn(self, token):
        """
        Establishes stomp connection to the machine selected by the user.

        Args:
            token: object containing the authorization token.

        Returns:
            client: client object subscribed to the desired topic.
        """

        headers = {
            'Authorization': 'Bearer ' + token['access_token'],
        }
        client = Client(self.brokerUrl)
        topic = '/topic/' + self.subscribed_machine['tenantId'] + '.' + self.subscribed_machine['id'] + '.gnb.telemetry'
        is_connected = False
        while not is_connected:
            print("Establishing STOMP connection...")
            is_connected = client.connect(headers=headers)
            time.sleep(0.5)

        client.subscribe(topic, callback=self.subscription_callback, headers=headers)

        return client

    def select_machine(self, machine_list):
        """
        Prompts the console until the user inputs a valid index representing a machine.

        Args:
            machine_list: list of machines available to the user.
        """

        print("Select the index of the machine you want to subscribe data from:")
        try:
            machine_number = int(input())
            while not machine_number >= 0 or not machine_number < len(machine_list):
                print("The number inserted does not match a machine, try again:")
                machine_number = int(input())
            self.subscribed_machine = machine_list[machine_number]
        except ValueError:
            print("Incorrect value.")
            self.select_machine(machine_list)

    def run(self):
        """
        Function that contains all the prompts necessary to establish a connection and subscription.
        """
        token = self.get_token()
        machine_list = self.get_machines(token['access_token'])

        print("You have the following machines available: ")
        for i, machine in enumerate(machine_list):
            print(f"{str(i)} - {machine['clientDescription']} ({machine['oaiboxType']}) [{machine['id']}]")

        self.select_machine(machine_list)

        print("Would you like to save the data to a file? (Y/N):")
        want_to_save = input().lower()
        while want_to_save != 'y' and want_to_save != 'n':
            print("Incorrect input type y/Y for Yes or n/N for no:")
            want_to_save = input().lower()

        if want_to_save == 'y':
            self.file_wanted = True
        else:
            self.graph_wanted = True

        self.client = self.conn(token)
        while not self.client.connected:
            continue
        self.connected = True
        print("Connected successfully!")
        print("Gathering Data", end='')
        while True:
            if self.client.connected:
                continue

    def quit(self):
        """
        Function to be called when the program is terminated, handles saving data to file if requested by the user.
        """
        self.client.disconnect()
        if self.file_wanted:
            telems_list = []
            timestamps = []
            for telem in self.sub_data.data:
                if telem['ues']:
                    ue_list = []
                    for ue in telem['ues']:
                        ue_list.append(ue)
                    telems_list.append(ue_list)
                    timestamps.append(telem['timestamp'])
            telem_df = pd.json_normalize(telems_list).set_axis(timestamps, axis='index')
            telem_df.to_csv(f"{os.getcwd()}/output/{str(int(time.time()))}.{self.subscribed_machine['id']}.csv")
            print(f"\nFile saved as {str(int(time.time()))}.{self.subscribed_machine['id']}.csv in output directory!")
        print("\nJob done.")


if __name__ == "__main__":

    data = SubscriberData()
    sub = Subscriber(data)
    try:
        sub.setDaemon(True)
        sub.start()
        while not sub.connected:
            continue
        if sub.graph_wanted:
            matplotlib.use('QtAgg')  # Backend GUI (different OS might require a change.)
            graph = LivePlot(data)
            graph.setDaemon(True)
            graph.start()
        while True:
            continue
    except (KeyboardInterrupt, SystemExit):
        sub.quit()
        exit()


