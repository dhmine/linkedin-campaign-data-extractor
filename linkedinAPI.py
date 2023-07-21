import requests
import sys
import json
from datetime import datetime, timedelta
import re
from urllib import parse
import argparse
import pandas as pd

# Function to parse command line arguments
def parse_arguments():
    parser = argparse.ArgumentParser(description='Process LinkedIn campaign data for a specific date range.')
    parser.add_argument('-s', '--start_date', help='The start date for the data pull in format YYYY-MM-DD', required=True)
    parser.add_argument('-e', '--end_date', help='The end date for the data pull in format YYYY-MM-DD', required=True)

    return parser.parse_args()

# Parse command line arguments
args = parse_arguments()
start_date = datetime.strptime(args.start_date, '%Y-%m-%d')
end_date = datetime.strptime(args.end_date, '%Y-%m-%d')


class LinkedInAdsManager:
    def __init__(self, access_token):
        self.access_token = access_token
        self.headers = {"Authorization": "Bearer " + self.access_token}

    def get_linkedin_ads_account(self):
        url =  "https://api.linkedin.com/v2/adAccountsV2?q=search&search.type.values[0]=BUSINESS&search.status.values[0]=ACTIVE"

        r = requests.get(url = url, headers = self.headers)

        account_df = pd.DataFrame(columns=["account_id","account_name", "account_currency"])

        if r.status_code != 200:
            print("\n ### something went wrong ### ", r)
            return None
        else:
            response_dict = json.loads(r.text)
            if "elements" in response_dict:
                accounts = response_dict["elements"]
                for acc in accounts:
                    account_df = account_df.append({
                        "account_id": acc["id"],
                        "account_name": acc["name"],
                        "account_currency": acc["currency"]
                    }, ignore_index=True)

        return account_df

    def get_LinkedIn_campaigns_list(self, account, camapign_type_json):
        url = "https://api.linkedin.com/v2/adCampaignsV2?q=search&search.account.values[0]=urn:li:sponsoredAccount:" + str(account)

        r = requests.get(url = url, headers = self.headers)

        campaign_data_df = pd.DataFrame(columns=["campaign_name", "campaign_id", "campaign_account",
                                                 "daily_budget", "unit_cost", "objective_type", "campaign_status", "campaign_type"])

        if r.status_code == 200:
            response_dict = json.loads(r.text)
            if "elements" in response_dict:
                campaigns = response_dict["elements"]
                for campaign in campaigns:
                    if "status" in campaign and campaign["status"] != "DRAFT":
                        campaign_data_df = campaign_data_df.append({
                            "campaign_name": campaign.get("name", "NA"),
                            "campaign_id": campaign.get("id", "NA"),
                            "campaign_account": re.findall(r'\d+', campaign.get("account", "NA"))[0] if campaign.get("account", "NA") else "NA",
                            "daily_budget": campaign.get("dailyBudget", {}).get("amount", None),
                            "unit_cost": campaign.get("unitCost", {}).get("amount", None),
                            "objective_type": campaign.get("objectiveType", None),
                            "campaign_status": campaign.get("status", "NA"),
                            "campaign_type": self._get_campaign_type(campaign.get("objectiveType", None), camapign_type_json)
                        }, ignore_index=True)

                campaign_data_df["daily_budget"] = pd.to_numeric(campaign_data_df["daily_budget"], errors='coerce')
                campaign_data_df["unit_cost"] = pd.to_numeric(campaign_data_df["unit_cost"], errors='coerce')

        return campaign_data_df

    def _get_campaign_type(self, campaign_obj, camapign_type_json):
        if campaign_obj in camapign_type_json["off_site"]:
            return "off_site"
        elif campaign_obj in camapign_type_json["on_site"]:
            return "on_site"
        else:
            print(" ### campaign ObjectiveType doesent match CampaignType references ###")
            return None

    def get_LinkedIn_campaign(self, campaigns_ids, s_date, e_date, qry_type):
        campaign_analytics_data = pd.DataFrame(columns=["campaign_id", "start_date", "end_date", "costInUsd",
                                                        "costInLocalCurrency", "dateRange", "sends", "impressions", "clicks"])

        for cmp_id in campaigns_ids:
            dateRange_start = f"dateRange.start.day={s_date.day}&dateRange.start.month={s_date.month}&dateRange.start.year={s_date.year}"
            dateRange_end = f"dateRange.end.day={e_date.day}&dateRange.end.month={e_date.month}&dateRange.end.year={e_date.year}"

            url = f"https://api.linkedin.com/v2/adAnalyticsV2?q=analytics&pivot=CAMPAIGN_GROUP&{dateRange_start}&{dateRange_end}"\
                f"&timeGranularity=DAILY&campaigns[0]=urn:li:sponsoredCampaign:{cmp_id}"
            url = f"https://api.linkedin.com/v2/adAnalyticsV2?q=analytics&pivot=CAMPAIGN&{dateRange_start}&{dateRange_end}"\
            f"&timeGranularity=DAILY&campaigns[0]=urn:li:sponsoredCampaign:"+str(cmp_id)+"&fields=dateRange,impressions,clicks,,costInLocalCurrency,costInUsd,pivotValues"
            r = requests.get(url=url, headers=self.headers)

            if r.status_code == 200:
                response_dict = json.loads(r.text)
                if "elements" in response_dict:
                    campaigns = response_dict["elements"]
                    for campaign in campaigns:
                        campaign_analytics_data = campaign_analytics_data.append({
                            "campaign_id": cmp_id,
                            "start_date": s_date,
                            "end_date": e_date,
                            "costInUsd": campaign.get("costInUsd", 0),
                            "costInLocalCurrency": campaign.get("costInLocalCurrency", 0),
                            "dateRange": campaign.get("dateRange", 0),
                            "sends": campaign.get("sends", 0),
                            "impressions": campaign.get("impressions", 0),
                            "clicks": campaign.get("clicks", 0),
                            qry_type: s_date.isocalendar()[1] if qry_type in ["week","weekly"] else s_date.month if qry_type in ["month","monthly"] else None
                        }, ignore_index=True)

        return campaign_analytics_data

if __name__ == "__main__":
    #reading LinkedIn credential json file
    cred_file = open("./ln_Credentials.json", 'r')
    cred_json = json.load(cred_file)
    access_token = cred_json["access_token"]
    ads_manager = LinkedInAdsManager(access_token)


    accounts_df = ads_manager.get_linkedin_ads_account()
    print(accounts_df)
    # assuming we are working with FP account
    account_id = "500080012"    

    # define campaign type
    campaign_type = {"on_site": ["LEAD_GEN", "VIDEO_VIEW", "JOB_APPLY"], "off_site": ["FOLLOW_COMPANY", "WEBSITE_VISIT", "ENGAGEMENT"]}

    campaigns_list = ads_manager.get_LinkedIn_campaigns_list(account_id, campaign_type)

    campaigns_ids = campaigns_list.campaign_id.values.tolist()

    campaigns = ads_manager.get_LinkedIn_campaign(campaigns_ids=campaigns_ids, s_date=start_date, e_date=end_date, qry_type='daily')
    campaigns.campaign_id = campaigns.campaign_id.astype(int)
    # merge the campaigns_list and campaings dataframes
    campaigns_list.campaign_id = campaigns_list.campaign_id.astype(int)
    df = campaigns_list.merge(campaigns, on='campaign_id', how='outer')

    """ df['costInLocalCurrency'] = df.costInLocalCurrency.astype(float)
    df['costInUsd'] = df.costInUsd.astype(float)
    print(df.groupby(['campaign_name', 'campaign_id'])['clicks', 'impressions', 
                                                       'costInLocalCurrency', 'costInUsd'].sum().reset_index().sort_values(by='costInLocalCurrency', ascending= False))
    df.to_excel('test1.xlsx')"""
    print(df )
   

