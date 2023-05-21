from random_user_agent.user_agent import UserAgent
from random_user_agent.params import SoftwareName, OperatingSystem, SoftwareType

def list_user_agents(): # genera una lista di user-agents da Chrome per PC Windows
    software_names = [SoftwareName.CHROME.value]
    operating_systems = [OperatingSystem.WINDOWS.value]
    software_types = [SoftwareType.WEB_BROWSER.value]
    user_agents = UserAgent(software_names=software_names, operating_systems=operating_systems, software_types=software_types, limit=1000).get_user_agents()
    return [ua["user_agent"].strip() for ua in user_agents]

if __name__ == "__main__":
    pass