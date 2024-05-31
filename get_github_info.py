import requests
import json
import os
from dotenv import load_dotenv


def main():
    load_dotenv()
    github_token = os.getenv("GITHUB_TOKEN")
    organization_name = "Scytale-exercise"
    base_url = "https://api.github.com"
    output_dir = "github_data"

    # create directory for JSON files if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)

    # fetch org info
    org_data = fetch_organization_info(base_url, github_token, organization_name)

    # fetch repositories for the organization
    repos_data = fetch_repositories(base_url, github_token, org_data)

    # write pull request data to json files
    write_repo_data_to_json(repos_data, github_token, output_dir)


def fetch_organization_info(base_url, github_token, organization_name):
    org_url = f"{base_url}/orgs/{organization_name}"
    headers = {
        'Authorization': f'token {github_token}',
        'Accept': 'application/vnd.github.v3+json'
    }
    org_response = requests.get(org_url, headers=headers)
    org_data = org_response.json()
    return org_data


def fetch_repositories(base_url, github_token, org_data):
    repos_url = org_data["repos_url"]
    headers = {
        'Authorization': f'token {github_token}',
        'Accept': 'application/vnd.github.v3+json'
    }
    repos_response = requests.get(repos_url, headers=headers)
    repos_data = repos_response.json()
    return repos_data


def write_repo_data_to_json(repos_data, github_token, output_dir):
    # fetch data from every repo
    for repo in repos_data:
        repository_id = repo["id"]
        repository_name = repo["name"]
        repository_owner = repo["owner"]["login"]

        # initialize page number to first page
        page = 1
        while True:
            prs_url = f"https://api.github.com/repos/{repository_owner}/{repository_name}/pulls?" \
                      f"state=all&page={page}"
            headers = {
                'Authorization': f'token {github_token}',
                'Accept': 'application/vnd.github.v3+json'
            }
            prs_response = requests.get(prs_url, headers=headers)
            prs_data = prs_response.json()

            if len(prs_data) != 0:
                prs_filename = f"{repository_name}_{repository_owner}_{page}_pulls.json"
                prs_path = os.path.join(output_dir, prs_filename)
                with open(prs_path, "w") as file:
                    json.dump(prs_data, file)
            else:
                break
            page += 1


if __name__ == "__main__":
    main()
